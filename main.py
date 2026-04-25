#imports
from mrjob.job import MRJob, MRStep #for MapReduce jobs
from mrjob.protocol import RawValueProtocol #raw bytes output for the final step (no JSON, no key)
# Prefer ujson (C-accelerated) for speed; fall back to stdlib json if the
# cluster image doesn't have ujson installed. Both expose .loads with the
# same signature for our usage.
try:
    import ujson as json_lib
except ImportError:
    import json as json_lib
import re #for tokenization
import heapq #for bounded top-k selection



class mrjob_ass1(MRJob):

    # Final-step output protocol: write only the value, as raw bytes,
    # with no key and no JSON encoding. This makes the mrjob output
    # itself match the assignment's required output.txt format, so no
    # post-processing script is needed. Inter-step serialization is
    # unaffected (still uses the default JSON internal protocol).
    OUTPUT_PROTOCOL = RawValueProtocol

    #for local testing purposes we need to give mrjob permission to use/upload files like stopwords.txt
    def configure_args(self):
        super(mrjob_ass1, self).configure_args()
        self.add_file_arg('--upload-file')


    #we define a pipeline of mappers and reducers. This way mrjob will automatically run the mappers and reducers in the correct order and handle the data flow between them.
    def steps(self):
        return [
            # Step 1: Preprocessing & Counting - Output format of this step is (category, word) as key and count as value
            MRStep(mapper_init=self.mapper_1_init,
                   mapper=self.mapper_1,
                   combiner=self.combiner_1,
                   reducer=self.reducer_1),

            # Step 2: The Chi-Square Calculation
            # mapper_2 regroups records by word so that reducer_2 can
            # see all per-category counts for the same word in one place, which
            # is required to compute word_total (A+B) and build the contingency table.
            # k/v in  : (category, word)        -> count
            #           (category, '__TOTAL__') -> cat_total
            # k/v out : word       -> (category, count)
            #           '__TOTAL__' -> (category, cat_total)
            MRStep(mapper=self.mapper_2,
                   reducer_init=self.reducer_2_init,
                   reducer=self.reducer_2,
                   jobconf={'mapreduce.job.reduces': '1'}),

            # Step 3: Chi-square computation, top-75 selection per category,
            # merged dictionary. The merged dictionary is built reducer-side
            # from the selected top-75 words per category (per the assignment's
            # "merge the lists" requirement) and emitted from reducer_final
            # under the '~DICT~' sentinel key. With multiple reducer tasks
            # each emits its own partial dictionary, which Step 4 unions
            # into a single line.
            MRStep(mapper=self.mapper_3,
                   combiner=self.combiner_3,
                   reducer_init=self.reducer_3_init,
                   reducer=self.reducer_3,
                   reducer_final=self.reducer_3_final),

            # Step 4: Final output assembly. Funnels every Step 3 record
            # under one sentinel key so a single reducer call sees all
            # category lines plus all '~DICT~' partials and can emit a
            # globally sorted, deduplicated final output. Combined with
            # OUTPUT_PROTOCOL = RawValueProtocol, this writes plain
            # output.txt-format lines directly (no JSON, no tabs, no
            # post-processing script needed). The data volume here is
            # tiny (~22 category lines plus a small number of partial
            # dict strings) so the single-reducer funnel is essentially
            # free.
            MRStep(mapper=self.mapper_4,
                   reducer=self.reducer_4),
        ]


    # =========================================================================
    # STEP 1 - Preprocessing and tokenization
    # =========================================================================

    splitting_on = re.compile(r'[()\\[\]{}.!?,;:+_"' + "'" + r'`~#@&*%€$§\\/\s\t\d=-]+')

    r'''
    Explanation:
    [ ... ] -> Everything inside these brackets is a delimiter
    \s\t\d  -> These are your whitespaces, tabs, and digits
    +-=_    -> The other required symbols
    +       -> This means "match one or more" (so "123" or "   " is treated as one split)
    '''

    def mapper_1_init(self): #to initialize the mapper with the set of stopwords provided
        self.stopwords = set()
        with open('stopwords.txt', 'r', encoding='utf-8') as file:
            for line in file:
                self.stopwords.add(line.strip().lower())


    def mapper_1(self, _, line):
        data = json_lib.loads(line) #parse the JSON line
        category = data.get('category') #get the category of the review
        review_text = data.get('reviewText', '') #get the review text, default to empty string if not present

        #convert to lowercase
        review_text = review_text.lower()

        # Tokenize and DEDUPLICATE within the document. Chi-square A is
        # document frequency (# of docs in category c that contain token t),
        # not term frequency. Emitting per occurrence would inflate A and
        # break the contingency table (A could exceed cat_total).
        seen = set()
        for word in self.splitting_on.split(review_text):
            if word and word not in self.stopwords and len(word) > 1:
                seen.add(word)

        for word in seen:
            yield (category, word), 1

        # Emit one document count per category so reducer_2 can compute
        # cat_total (= A+C in the chi-square contingency table) and grand total N.
        # This is forwarded by mapper_2 under the same '__TOTAL__' key.
        yield (category, '__TOTAL__'), 1


    def combiner_1(self, key, counts): #combiner to sum up counts locally before sending to reducer - this improves efficiency
        yield key, sum(counts)


    def reducer_1(self, key, counts): #reducer to sum up counts globally
        yield key, sum(counts)


    # =========================================================================
    # STEP 2 - Chi-Square Mapper
    # =========================================================================
    # Chi-square requires building a contingency table for each (token, category) pair:
    #   A = number of documents in category c that contain token t
    #   B = cat_total - A
    #       documents in category c that do NOT contain token t
    #   C = token_total - A
    #       documents outside category c that contain token t
    #   D = N - cat_total - token_total + A
    #       documents outside category c that do NOT contain token t
    #   cat_total   = total documents in category c
    #   token_total = total documents containing t across ALL categories
    #   N           = grand total documents across all categories
    #
    # Chi-square formula:
    #   chi2(t,c) = N * (A*D - B*C)^2 / (token_total * (N - token_total)
    #                                     * cat_total  * (N - cat_total))
    #
    # Design rationale:
    # After Step 1, records are keyed by (category, word). To compute
    # token_total (= A + C, documents containing t across ALL categories),
    # all per-category counts for the same word must arrive at the same
    # reducer. This requires rekeying by word (= token).
    # Category totals are forwarded alongside so reducer_2 has all values
    # (A, cat_total, token_total, N) needed to compute chi-square.
    #
    # k/v in  : (category, word)        -> A           word records
    #           (category, '__TOTAL__') -> cat_total   category total records
    # k/v out : word        -> (category, A)           regroup by token
    #           '__TOTAL__' -> (category, cat_total)   forward cat totals
    # =========================================================================

    def mapper_2(self, key, count):
        """
        Rekeys word records by token so that reducer_2 receives all
        per-category counts for the same token together and can compute
        token_total = sum(A across all categories).

        Category total records ('__TOTAL__') are forwarded so reducer_2
        can collect cat_total per category and derive N = sum(cat_totals).

        k/v in  : (category, word)        -> A           word records
                  (category, '__TOTAL__') -> cat_total   category totals
        k/v out : word        -> (category, A)           regroup by token
                  '__TOTAL__' -> (category, cat_total)   forward totals
        """
        cat, word = key
        if word == '__TOTAL__':
            # Forward category total records so reducer_2 can collect all
            # cat_totals and compute N = sum(cat_totals).
            yield '__TOTAL__', (cat, count)
        else:
            # Rekey by word so all (category, A) pairs for the same word
            # arrive at the same reducer task.
            yield word, (cat, count)


    # reducer_2 relies on '__TOTAL__' being processed before any word key.
    # With a single reducer (--jobconf mapreduce.job.reduces=1 on Hadoop;
    # local mode is inherently single-reducer), mrjob sorts JSON-encoded
    # keys and '"__TOTAL__"' sorts before any lowercase word, so cat_totals
    # and N are fully populated when word keys arrive.

    def reducer_2_init(self):
        """
        Initialises shared state used across all reducer_2 calls.
        cat_totals is populated when key=='__TOTAL__' (always processed
        first due to sort order + single-reducer constraint), so the
        values are available by the time word keys arrive.

        k/v in  : none
        k/v out : none (initialises self.cat_totals and self.N)
        """
        self.cat_totals = {}   # {category: total_doc_count_in_category}
        self.N = 0             # grand total documents across all categories

    def reducer_2(self, key, values):
        """
        Handles two record types, sorted by key:

        1. key == '__TOTAL__' arrives first. Populates self.cat_totals
           from each (category, cat_total) pair and accumulates N.
           Emits nothing, just builds state.

        2. key == word (any token string). Computes
           token_total = sum(A across all categories), then emits one
           record per (category, A) with all four values needed for
           chi-square in Step 3.

        k/v in  : '__TOTAL__' -> (category, cat_total)
                  word        -> (category, A)
        k/v out : (category, word) -> [A, token_total, cat_total, N]
                  (nothing emitted for '__TOTAL__' key)
        """
        if key == '__TOTAL__':
            for cat, cat_total in values:
                self.cat_totals[cat] = cat_total
                self.N += cat_total
        else:
            word = key
            word_data = list(values)
            token_total = sum(A for _, A in word_data)
            for cat, A in word_data:
                cat_total = self.cat_totals.get(cat, 0)
                if cat_total > 0 and self.N > 0:
                    yield (cat, word), [A, token_total, cat_total, self.N]



    # =========================================================================
    # STEP 3 - Chi-square, top-75 per category, merged dictionary
    # =========================================================================

    def _top_75(self, values):
        """
        Returns the best 75 (chi2, word) pairs without materialising and
        sorting all values. Ordering matches sorted(values,
        key=lambda x: (-x[0], x[1]))[:75].
        """
        return heapq.nsmallest(75, values, key=lambda x: (-x[0], x[1]))

    def mapper_3(self, key, value):
        """
        Computes chi-square for each (category, word) pair using the
        contingency table values emitted by reducer_2. Rekeys by
        category so reducer_3 can collect all (chi2, word) pairs per
        category and pick the top 75.

        Skips emission when the chi-square denominator is 0, which
        happens only in degenerate cases (e.g. a token appears in
        every document or a category has zero documents).

        The merged dictionary is NOT emitted from this mapper. It is
        built reducer-side from the selected top-75 words per
        category and emitted in reducer_3_final, so the dictionary
        contains exactly the union of the top-75 lists (per the
        assignment's "merge the lists" requirement) rather than the
        full post-stopword vocabulary.

        k/v in  : (category, word) -> [A, token_total, cat_total, N]
        k/v out : category -> (chi2, word)   for top-75 selection
        """
        cat, word = key
        A, token_total, cat_total, N = value

        B = cat_total - A
        C = token_total - A
        D = N - cat_total - token_total + A

        denom = token_total * (N - token_total) * cat_total * (N - cat_total)
        if denom == 0:
            return

        chi2 = N * (A * D - B * C) ** 2 / denom

        yield cat, (chi2, word)


    def combiner_3(self, key, values):
        """
        Reduces shuffle volume between mapper_3 and reducer_3 by
        keeping only the local top-75 by chi2 per category. Any
        (chi2, word) outside the local top-75 for a mapper task
        cannot be in the global top-75 for that category, so it is
        safe to drop here.

        Sort key (-chi2, word) breaks chi2 ties alphabetically so
        the selection is deterministic across runs even when the
        shuffle order varies.

        k/v in  : category -> (chi2, word)
        k/v out : category -> (chi2, word)   local top-75 per mapper
        """
        top = self._top_75(values)
        for item in top:
            yield key, item


    def reducer_3_init(self):
        """
        Initialises the set used to build the merged dictionary
        line. Populated as each category is processed in reducer_3
        and emitted once in reducer_3_final.

        k/v in  : none
        k/v out : none (initialises self.dict_words)
        """
        self.dict_words = set()


    def reducer_3(self, key, values):
        """
        Selects the top-75 (chi2, word) pairs by chi2 descending for
        the category, accumulates the selected words into
        self.dict_words for the final merged dictionary line, and
        emits the formatted category line.

        Sort key (-chi2, word) breaks chi2 ties alphabetically so
        the global top-75 is reproducible across runs.

        chi2 is rendered using :.10g (up to 10 significant digits,
        no trailing zeros, no scientific notation for typical
        magnitudes).

        k/v in  : category -> [(chi2, word), ...]
        k/v out : category -> "Category word1:chi2 word2:chi2 ..."
        """
        cat = key
        top_75 = self._top_75(values)
        self.dict_words.update(word for _, word in top_75)
        parts = [f'{word}:{chi2:.10g}' for chi2, word in top_75]
        yield cat, f'{cat} {" ".join(parts)}'


    def reducer_3_final(self):
        """
        Emits this reducer task's partial merged dictionary once
        after all of its category keys have been processed. With
        multiple Step 3 reducer tasks each task contributes its own
        partial; Step 4 unions them into the final dictionary line.

        Uses the '~DICT~' sentinel key.

        k/v in  : none
        k/v out : '~DICT~' -> "word1 word2 ..." (partial, alphabetical)
        """
        yield '~DICT~', ' '.join(sorted(self.dict_words))


    # =========================================================================
    # STEP 4 - Final output assembly (single-reducer funnel + raw output)
    # =========================================================================

    def mapper_4(self, key, value):
        """
        Routes every Step 3 record under one sentinel key so all of
        them land in a single reducer call in Step 4. With only one
        unique map-output key, the partitioner sends all records to
        a single reducer regardless of how many reducer tasks the
        runner launches; the other reducer tasks receive no keys
        and produce no output.

        Step 3 emits at most ~22 category records plus one '~DICT~'
        record per Step 3 reducer task, so the funnel volume is
        trivial.

        k/v in  : category -> "Category word1:chi2 ..."
                  '~DICT~' -> "word1 word2 ..." (partial dict)
        k/v out : '__OUT__' -> (original_key, original_value)
        """
        yield '__OUT__', (key, value)


    def reducer_4(self, key, values):
        """
        Receives every record from Step 3 in a single call. Emits
        category lines in alphabetical order followed by the merged
        dictionary line built from the union of all '~DICT~'
        partials.

        Uses OUTPUT_PROTOCOL = RawValueProtocol, so each yielded
        value is written as raw bytes (one line per yield) with no
        key and no JSON encoding. This produces the exact
        output.txt format required by the assignment without any
        post-processing.

        k/v in  : '__OUT__' -> [(orig_key, orig_value), ...]
        k/v out : None -> "Category word1:chi2 ..."   (per category)
                  None -> "word1 word2 ..."           (merged dict)
        """
        category_lines = []
        dict_words = set()
        for orig_key, orig_value in values:
            if orig_key == '~DICT~':
                dict_words.update(orig_value.split())
            else:
                category_lines.append((orig_key, orig_value))

        category_lines.sort(key=lambda kv: kv[0])
        for _, line in category_lines:
            yield None, line
        yield None, ' '.join(sorted(dict_words))


# This is the main entry point for the script when run from the command line
if __name__ == '__main__':
    mrjob_ass1.run()

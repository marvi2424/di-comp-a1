#imports
from mrjob.job import MRJob, MRStep #for MapReduce jobs
import ujson #for more efficient JSON parsing
import re #for tokenization



class mrjob_ass1(MRJob):

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
            # merged dictionary. '~DICT~' sentinel sorts last (tilde ASCII 126
            # > all uppercase category letters) so the merged line appears last.
            MRStep(mapper=self.mapper_3,
                   combiner=self.combiner_3,
                   reducer=self.reducer_3),
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
        data = ujson.loads(line) #parse the JSON line
        category = data.get('category') #get the category of the review
        review_text = data.get('reviewText', '') #get the review text, default to empty string if not present

        #convert to lowercase
        review_text = review_text.lower()

        #tokenize the review text using the defined splitting regex
        for word in self.splitting_on.split(review_text):
            if word and word not in self.stopwords and len(word) > 1:
                yield (category, word), 1 #emit (category, word) as key and 1 as value for counting

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

    def mapper_3(self, key, value):
        """
        Computes chi-square for each (category, word) pair using the
        contingency table values emitted by reducer_2. Rekeys by
        category so reducer_3 can collect all (chi2, word) pairs per
        category and pick the top 75. Also emits every word under the
        '~DICT~' sentinel key so reducer_3 can build the merged
        dictionary line (tilde sorts after all uppercase category
        names, so the merged line lands last).

        Skips emission when the chi-square denominator is 0, which
        happens only in degenerate cases (e.g. a token appears in
        every document or a category has zero documents).

        k/v in  : (category, word) -> [A, token_total, cat_total, N]
        k/v out : category  -> (chi2, word)   for top-75 selection
                  '~DICT~'  -> word           for merged dictionary
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
        yield '~DICT~', word


    def combiner_3(self, key, values):
        """
        Reduces shuffle volume between mapper_3 and reducer_3.

        For category keys, keeps only the local top-75 by chi2, since
        any (chi2, word) outside the local top-75 cannot be in the
        global top-75 for that category.

        For the '~DICT~' key, deduplicates words locally so each
        unique word travels to the reducer at most once per mapper
        task.

        Yields the same k/v shape as mapper_3: individual tuples for
        category keys, individual words for '~DICT~'.

        k/v in  : category  -> (chi2, word)
                  '~DICT~'  -> word
        k/v out : category  -> (chi2, word)   local top-75 per mapper
                  '~DICT~'  -> word           deduplicated per mapper
        """
        if key == '~DICT~':
            seen = set()
            for word in values:
                if word not in seen:
                    seen.add(word)
                    yield key, word
        else:
            top = sorted(values, key=lambda x: -x[0])[:75]
            for item in top:
                yield key, item


    def reducer_3(self, key, values):
        """
        Builds the final output lines.

        For category keys, sorts the incoming (chi2, word) pairs by
        chi2 descending, keeps the top 75, and formats the line as
        "Category word1:chi2 word2:chi2 ..." with chi2 rendered
        using :.10g (up to 10 significant digits, no trailing zeros,
        no scientific notation for typical magnitudes).

        For '~DICT~' (arrives last, tilde > uppercase ASCII),
        deduplicates, sorts alphabetically, and joins with spaces to
        produce the merged dictionary line.

        k/v in  : category -> [(chi2, word), ...]
                  '~DICT~' -> [word, ...]
        k/v out : category -> "Category word1:chi2 word2:chi2 ..."
                  '~DICT~' -> "word1 word2 ..." (alphabetical)
        """
        if key == '~DICT~':
            all_words = sorted(set(values))
            yield key, ' '.join(all_words)
        else:
            cat = key
            top_75 = sorted(values, key=lambda x: -x[0])[:75]
            parts = [f'{word}:{chi2:.10g}' for chi2, word in top_75]
            yield cat, f'{cat} {" ".join(parts)}'


# This is the main entry point for the script when run from the command line
if __name__ == '__main__':
    mrjob_ass1.run()

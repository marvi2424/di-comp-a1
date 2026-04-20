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
                   reducer=self.reducer_2),
            # TODO add Step 3 for top-75 selection and output formatting
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


    # TODO implement reducer_2
    # Expected inputs:
    #   key='__TOTAL__' : values = [(category, cat_total), ...]
    #                     -> compute N = sum(cat_totals)
    #                     -> emit (category) -> (cat_total, N)
    #   key=word        : values = [(category, A), ...]
    #                     -> compute token_total = sum(A)
    #                     -> emit (category, word) -> (A, token_total, cat_total, N)
    #
    # Expected outputs (needed by Step 3 to compute chi-square):
    #   (category, word) -> (A, token_total, cat_total, N)

    def reducer_2(self, key, values):
        # TEMPORARY!! Remove once real reducer is implemented.
        # Just forwards all values so we can inspect mapper_2 output
        for v in values:
            yield key, v



    # TODO implement Step 3 (chi-square formula, top-75 per category, output formatting)


# This is the main entry point for the script when run from the command line
if __name__ == '__main__':
    mrjob_ass1.run()

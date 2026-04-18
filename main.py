#imports
from mrjob.job import MRJob, MRStep #for MapReduce jobs
import ujson #for more efficient JSON parsing
import re #for tokenization



class mrjob_ass1(MRJob):
    
    #for local testing purposes we need to give mrjob  permission to use/upload files like stopwords.txt
    def configure_args(self):
        super(mrjob_ass1, self).configure_args()
        self.add_file_arg('--upload-file')
    
    
    #we define a pipeline of mappers and reducers. This way mrjob will automatically run the mappers and reducers in the correct order and handle the data flow between them.
    def steps(self):
        return [
            # Step 1: Preprocessing & Counting -Output format of this step is (category, word) as key and count as value
            MRStep(mapper_init=self.mapper_1_init,
                   mapper=self.mapper_1,
                   combiner=self.combiner_1,
                   reducer=self.reducer_1),
            
            # Step 2: The Chi-Square Calculation
            #add more steps here as needed
            #format like
            #MRStep(reducer=self.reducer_2_chisquare
            #        .
            #        .
            #        .
        ]
        
        
    #Step 1 Preprocessing and tokenization
    
    splitting_on = re.compile(r'[()\\[\]{}.!?,;:+_"' + "'" + r'`~#@&*%€$§\\/\s\t\d=-]+')
    
    '''
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
    
    
    def combiner_1(self, key, counts): #combiner to sum up counts locally before sending to reducer - this improves efficiency
        yield key, sum(counts)
    
    
    def reducer_1(self, key, counts): #reducer to sum up counts globally
        yield key, sum(counts)
    
    
    #CONTINUE HERE:
    # Here you can add more reducers (reducer_2, reducer_3, etc.) or mappers if needed
    # Here we need to implement the logic to calculate the chi-square statistic for each (category, word) pair based on the counts obtained from the first step.
    # This would likely involve another reducer that takes the counts and computes the chi-square values, and then emits the top 75 terms per category based on those values.
    # When done, add the reducer to the steps() method to ensure it gets executed after the counting step.
    
    
    
    
    
    





# This is the main entry point for the script when run from the command line
if __name__ == '__main__':
    mrjob_ass1.run()
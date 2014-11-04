import luigi
from luigi import configuration
from luigi.s3 import S3Target, S3PathTask

from mortar.luigi import mortartask


"""

To run, replace <your-handle-here> with something unique to you (for instance, first initial plus last name):
    
    mortar luigi luigiscripts/word-luigi-solution.py \
        --output-path s3://mortar-example-output-data/<your-handle-here>/q-words
"""

class WordRank(mortartask.MortarProjectPigscriptTask):

    # Cluster size to use for running this Pig script
    # Defaults to Local Mode (no cluster), 
    # but larger cluster values can be passed in at runtime
    cluster_size = luigi.IntParameter(default=0)

    # S3 path to the script's output directory (will be passed in as a parameter at runtime)
    output_path = luigi.Parameter()

    def token_path(self):
        """
        S3 path for Luigi to store tokens indicating 
        the Task has completed.
        """
        return self.output_path

    def script(self):
        """
        Name of the Pigscript to run 
        (omit the .pig from the file name).
        """
        return 'google_books_words'

    def parameters(self):
        """
        Any parameters that you want to pass to your Pig script can go here.
        In this case we want the Pig job to write output to the same path
        Luigi is using, which will make it easier for subsequent Luigi tasks to find
        the output from the Pig job.
        """
        return {
            'OUTPUT_PATH': self.output_path
        }

    def requires(self):
        """
        Dependency of this Task -- in this case Luigi uses 
        S3PathTask to check for the existence of input data
        at a certain S3 path before running this Task.
        """
        return [S3PathTask('s3://mortar-example-data/ngrams/books/20120701/eng-all/1gram/googlebooks-eng-all-1gram-20120701-q.gz')]

    def script_output(self):
        """
        S3 target where the Pig job's output will be stored. 
        This directory will be cleared of partial data 
        if the script fails.
        """
        return [S3Target(self.output_path)]

class SanityTest(luigi.Task):

    output_path = luigi.Parameter()

    def requires(self):
        """
        This Task takes the output of the WordRank Task as its input, 
        so we list WordRank as a dependency.
        """
        return [WordRank(output_path=self.output_path)]

    def output(self):
        """
        We want this Task to write its tokens to a unique location, 
        defined by the name of the class.
        """
        return [S3Target('%s/%s' % (self.output_path, self.__class__.__name__))]

    def run(self):
        """
        This Python code checks that each word in the pig script 
        output file begins with 'q' and returns an exception if not.
        """
        file = S3Target('%s/%s/part-r-00000' % (self.output_path, 'dictionary'))
        for line in file.open('r'):
            if line[0] != 'q':
                raise Exception("Word: %s didn't start with q" % word)

if __name__ == "__main__":
    luigi.run(main_task_cls=SanityTest)

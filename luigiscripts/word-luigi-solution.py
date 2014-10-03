import luigi
from luigi import configuration
from luigi.s3 import S3Target, S3PathTask

from mortar.luigi import mortartask


"""

To run, replace <your-handle-here> with something unique to you (for instance, first initial plus last name):
    
    mortar local:luigi luigiscripts/word-luigi-solution.py --output-path s3://mortar-example-output-data/<your-handle-here>/q-words

"""

class WordRank(mortartask.MortarProjectPigscriptTask):

    # Cluster size to use for running this Pig script
    # Defaults to Local Mode (no cluster), but larger cluster values can be passed in at runtime
    cluster_size = luigi.IntParameter(default=0)

    # S3 path to the script's output directory (will be passed in as a parameter at runtime)
    output_path = luigi.Parameter()

    # S3 path for Luigi to store tokens indicating the task has completed
    def token_path(self):
        return self.output_path

    # Name of the Mortar project where Pig script exists
    def project(self):
        """
        Add your project's name here
        """
        return 'my-project-name'

    # Name of the Pig script to run (omit the .pig from the file name)
    def script(self):
        return 'google_books_words'

    # Any parameters that you want to pass to your Pig script can go here.
    # In this case we want the Pig job to write output to the same path
    # Luigi is using, which will make it easier for subsequent Luigi tasks to find
    # the output from the Pig job
    def parameters(self):
        return {'OUTPUT_PATH': self.output_path}

    # Dependency of this task -- in this case Luigi uses S3PathTask to check for the existence of 
    # input data at a certain S3 path before running this task
    def requires(self):
        return [S3PathTask('s3://mortar-example-data/ngrams/books/20120701/eng-all/1gram/googlebooks-eng-all-1gram-20120701-q.gz')]

    # S3 target where the Pig job's output will be stored. 
    # This directory will be cleared of partial data if the script fails
    def script_output(self):
        return [S3Target(self.output_path)]

class SanityTest(luigi.Task):

    output_path = luigi.Parameter()

    # This task takes the output of the WordRank task as its input, 
    # so we list WordRank as a dependency
    def requires(self):
        return [WordRank(output_path=self.output_path)]

    # We want this task to write its tokens to a unique location, defined by the name of the class
    def output(self):
        return [S3Target('%s/%s' % (self.output_path, self.__class__.__name__))]

    # This Python code checks that each word in the file begins with 'q' 
    # and returns an exception if not
    def run(self):
        file = S3Target('%s/%s/part-r-00000' % (self.output_path, 'dictionary'))
        for line in file.open('r'):
            if line[0] != 'q':
                raise Exception("Word: %s didn't start with Q" % word)

if __name__ == "__main__":
    luigi.run(main_task_cls=SanityTest)

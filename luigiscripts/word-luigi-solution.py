import luigi
from luigi import configuration, LocalTarget
from luigi.s3 import S3Target, S3PathTask

from mortar.luigi import mortartask, s3transfer


"""

To run, replace <your-email-here> with the email address associated with your Mortar account, 
    replacing the symbols in your email with dashes (e.g., user-mortardata-com):
    
    mortar local:luigi luigiscripts/word-luigi-solution.py --s3-path s3://mortar-example-output-data/<your-email-here>/1gram/dictionary

"""

class WordCount(mortartask.MortarProjectPigscriptTask):

    # Cluster size to use for running this Pig script
    # Defaults to Local Mode (no cluster), but larger cluster values can be passed in at runtime
    cluster_size = luigi.IntParameter(default=0)

    # S3 path to the script's output directory (will be passed in as a parameter at runtime)
    s3_path = luigi.Parameter()

    # S3 path for Luigi to store tokens indicating the task has completed
    def token_path(self):
        return self.s3_path

    # Name of the Mortar project where Pig script exists
    def project(self):
        """
        Add your project's name here
        """
        return 'your-project-name'

    # Name of the Pig script to run (omit the .pig from the file name)
    def script(self):
        return 'google_books_words'

    # Dependency of this task -- in this case Luigi uses S3PathTask to check for the existence of 
    # input data at a certain S3 path before running this task
    def requires(self):
        return [S3PathTask('s3://mortar-example-data/ngrams/books/20120701/eng-all/1gram/googlebooks-eng-all-1gram-20120701-q.gz')]

    # S3 target where the Pig job's output will be stored. 
    # This directory will be cleared of partial data if the script fails
    def script_output(self):
        return [S3Target(self.s3_path)]

class CopyLocal(s3transfer.S3ToLocalTask):

    # path to the local directory where output will be transferred
    local_path = luigi.Parameter(default='data/output')

    file_name = luigi.Parameter(default='part-r-00000')

    s3_path = luigi.Parameter()

    def requires(self):
        return [WordCount(s3_path=self.s3_path)]

if __name__ == "__main__":
    luigi.run(main_task_cls=CopyLocal)

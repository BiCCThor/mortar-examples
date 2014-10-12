import os.path
import tempfile

import luigi
from luigi.s3 import S3Client

from mortar.luigi.mortartask import MortarRTask
from mortar.luigi.s3transfer import S3ToLocalTask, LocalToS3Task

import logging
logger = logging.getLogger('luigi-interface')

"""
This luigi pipeline pulls down Olympic medal data from S3, runs an R script
to graph it, uploads the graph back to S3, and then generates a URL 
to download the graph from S3.

Task Order:
    CopyOlympicsDataFromS3
    PlotOlympicsData
    CopyOlympicMedalsGraphToS3
    PrintS3GraphLink

To run:
    mortar luigi luigiscripts/olympics-luigi.py \
      --s3-path "s3://mortar-example-output-data/<your-handle-here>/medals_graph.png"
"""


# local temporary directory
LOCAL_TEMP_DIR = tempfile.gettempdir()

def get_temp_file(filename):
    """
    Helper method to get a path to a file in the local temporary
    directory.
    """
    return os.path.join(LOCAL_TEMP_DIR, filename)

class CopyOlympicsDataFromS3(S3ToLocalTask):
    """
    Copy the Olympics data from S3 down to the local filesystem.
    """

    # source path for the data in S3
    s3_path = luigi.Parameter(default='s3://mortar-example-data/olympics/OlympicAthletes.csv')

    # target path for the data on local file system
    local_path = luigi.Parameter(default=get_temp_file('Olympics.csv'))

    def requires(self):
        """
        Previous Tasks that this Task depends on. We have no dependencies.
        """
        return []

class PlotOlympicsData(MortarRTask):
    """
    Run an R script to plot the Olympics data (using ggplot2).
    """

    # path for storing a token to tell Luigi that the task is complete.
    # we will will not be using this, as our R script will produce actual
    # output that we can ask Luigi to check for
    token_path = luigi.Parameter(default=LOCAL_TEMP_DIR)

    # local file where R should store the graph
    olympic_medals_graph_path = luigi.Parameter()

    def rscript(self):
        """
        R script to run. This should be a path relative to the 
        root directory of your Mortar project.
        """
        return 'rscripts/olympics.R'

    def arguments(self):
        """
        List of arguments to be sent to your R script.
        """
        # grab the path to the local file created by 
        # CopyOlympicsDataFromS3
        local_olympics_data_file = self.input()[0][0].path

        # tell our script to read data from the local file
        # and write it out to the graph path
        return [local_olympics_data_file, self.olympic_medals_graph_path]

    def requires(self):
        """
        This task requires that the Olympics data
        has first been copied from S3.
        """
        # require that data be moved in from S3
        return [CopyOlympicsDataFromS3()]

    def output(self):
        """
        This task will output a graph at the olympic_medals_graph_path,
        so we tell Luigi to look for that to know whether the task has
        already completed.
        """
        return luigi.LocalTarget(self.olympic_medals_graph_path)

class CopyOlympicMedalsGraphToS3(LocalToS3Task):
    """
    Copy the Olympic medal graph from local storage up to S3.
    """

    # target S3 path where the graph should land
    s3_path = luigi.Parameter()

    # source local path where the graph lives
    local_path = luigi.Parameter(default=get_temp_file('medals_graph.png'))

    def requires(self):
        """
        Require that the graph has been generated before we copy it.
        """
        # we pass the path for the graph to PlotOlympicsData so
        # it knows where to store it
        return [PlotOlympicsData(olympic_medals_graph_path=self.local_path)]

class PrintS3GraphLink(luigi.Task):
    """
    Fetch an https URL for the graph from S3 so that
    we can download it easily.
    """

    # S3 path where the graph will be stored
    s3_path = luigi.Parameter()

    # how long to keep the https URL we generate active
    url_expires_in = luigi.IntParameter(default=60*60*24)
    
    def requires(self):
        """
        Require that the graph has been copied to S3 before
        we try to print a URL for it.
        """
        return [CopyOlympicMedalsGraphToS3(s3_path=self.s3_path)]

    def output(self):
        """
        Trick luigi into re-running this Task every time by
        pointing it at a file that never exists. (how clever!)
        """
        return [luigi.LocalTarget(get_temp_file('does-not-exist-so-task-will-always-run'))]

    def run(self):
        """
        Generate and print a URL where we can download the graph.
        """
        s3_client = S3Client()
        s3_key = s3_client.get_key(self.s3_path)
        download_url = s3_key.generate_url(expires_in=self.url_expires_in)

        logger.info('DOWNLOAD GRAPH AT: %s' % download_url)

if __name__ == "__main__":
    # Tell luigi to run the PrintS3GraphLink task. It will
    # expand the chain of dependencies using the "requires" method
    # for each Task
    luigi.run(main_task_cls=PrintS3GraphLink)

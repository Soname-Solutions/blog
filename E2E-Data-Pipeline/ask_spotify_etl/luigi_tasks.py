import luigi
import logging
import os

from luigi.contrib.mysqldb import MySqlTarget
from ask_spotify_etl_config import Config

project_root = os.path.dirname(os.path.abspath(__file__))
logging.config.fileConfig(fname=os.path.join(project_root, 'config', 'logging.conf'))
logger = logging.getLogger()
config = Config()


class AskSpotifyPipeline(luigi.Task):

    files_to_process = luigi.ListParameter()

    def get_luigi_target(self):
        return MySqlTarget(host=config.get('database.credentials', 'host'),
                    database=config.get('database.credentials', 'database'),
                    user=config.get('database.credentials', 'user'),
                    password=config.get('database.credentials', 'password'),
                    table=self.__class__.__name__,
                    update_id=1) #TODO: generated sequence id must be provided here

    def run(self):
        logger.info(f"{self.files_to_process} will be in process")
        import time; time.sleep(5)

    def output(self):
        return self.get_luigi_target().touch()

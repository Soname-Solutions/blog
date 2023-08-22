import luigi
import logging
import os

from luigi.contrib.mysqldb import MySqlTarget
from ask_spotify_etl_config import Config

from random import randint #TODO: remove me

project_root = os.path.dirname(os.path.abspath(__file__))
logging.config.fileConfig(fname=os.path.join(project_root, 'config', 'logging.conf'))
logger = logging.getLogger()
config = Config()

class LuigiMaridbTarget():
    """get luigi DB target to log task state"""

    def get_luigi_target(self, table, control_value):
        return MySqlTarget(host=config.get('database.credentials', 'host'),
                    database=config.get('database.credentials', 'database'),
                    user=config.get('database.credentials', 'user'),
                    password=config.get('database.credentials', 'password'),
                    table=table,
                    update_id=control_value)

class ETLControlRegistration(luigi.Task, LuigiMaridbTarget):

    file = luigi.Parameter()

    def run(self):
        logger.info(f"I am regestr files into etl_control table for {self.file}")
        return self.get_luigi_target(
            table=__class__.__name__,
            control_value=__class__.__name__ + self.file + '1'
        ).touch()

    def output(self):
        return self.get_luigi_target(
            table=__class__.__name__,
            control_value=__class__.__name__ + self.file + '1'
        )

class LALoadTask(luigi.Task, LuigiMaridbTarget):

    file = luigi.Parameter()

    def requires(self):
        return ETLControlRegistration(file=self.file)

    def run(self):
        logger.info(f"I am Loading files into LA table for {self.file}")
        return self.get_luigi_target(
            table=__class__.__name__,
            control_value=__class__.__name__ + self.file + '1'
        ).touch()

    def output(self):
        return self.get_luigi_target(
            table=__class__.__name__,
            control_value=__class__.__name__ + self.file + '1'
        )

class TRLoadTask(luigi.Task, LuigiMaridbTarget):

    file = luigi.Parameter()

    def requires(self):
        return LALoadTask(file=self.file)

    def run(self):
        logger.info(f"I am loading data into TR table for {self.file}")
        return self.get_luigi_target(
            table=__class__.__name__,
            control_value=__class__.__name__ + self.file + '1'
        ).touch()

    def output(self):
        return self.get_luigi_target(
            table=__class__.__name__,
            control_value=__class__.__name__ + self.file + '1'
        )

class DSLoadTask(luigi.Task, LuigiMaridbTarget):

    file = luigi.Parameter()

    def requires(self):
        return TRLoadTask(file=self.file)

    def run(self):
        logger.info(f"I am loading data into ds table for {self.file}")
        return self.get_luigi_target(
            table=__class__.__name__,
            control_value=__class__.__name__ + self.file + '1'
        ).touch()

    def output(self):
        return self.get_luigi_target(
            table=__class__.__name__,
            control_value=__class__.__name__ + self.file + '1'
        )

class AskSpotifyPipeline(luigi.Task, LuigiMaridbTarget):
    """pipeline reversed entry point."""

    files_to_process = luigi.ListParameter()


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.control_value = '-'.join(sorted(self.files_to_process))

    def requires(self):
        for file in self.files_to_process:
            yield DSLoadTask(file=file)

    def run(self):
        logger.info(f"{self.files_to_process} are processed by luigi pipeline ")
        return self.get_luigi_target(
            table=__class__.__name__,
            control_value=self.control_value).touch()

    def output(self):
        return self.get_luigi_target(
            table=__class__.__name__,
            control_value=self.control_value)

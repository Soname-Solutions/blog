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
    def run(self):
        return self.get_luigi_target(
            table=self.class_name,
            control_value=self.control_value
        ).touch()

    def output(self):
        return self.get_luigi_target(
            table=self.class_name,
            control_value=self.control_value
        )

class ETLControlRegistration(LuigiMaridbTarget, luigi.Task):

    file = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.class_name = __class__.__name__


class LALoadTask(LuigiMaridbTarget, luigi.Task):

    file = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.class_name = __class__.__name__

    def requires(self):
        return ETLControlRegistration(file=self.file)


class TRLoadTask(LuigiMaridbTarget, luigi.Task):

    file = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.class_name = __class__.__name__

    def requires(self):
        return LALoadTask(file=self.file)


class DSLoadTask(LuigiMaridbTarget, luigi.Task):

    file = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.class_name = __class__.__name__
        self.control_value = self.class_name + self.file + '1' # TODO: control value to be defined

    # def requires(self):
    #     return TRLoadTask(file=self.file)


class AskSpotifyPipeline(LuigiMaridbTarget, luigi.Task):
    """pipeline reversed entry point."""

    files_to_process = luigi.ListParameter()


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.class_name = __class__.__name__
        self.control_value = '-'.join(sorted(self.files_to_process))

    def requires(self):
        for file in self.files_to_process:
            yield DSLoadTask(file=file)

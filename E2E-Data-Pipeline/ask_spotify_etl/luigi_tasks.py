import csv
import luigi
import logging
import os

from luigi.contrib.mysqldb import MySqlTarget
from ask_spotify_etl_config import Config
from db_connector.mariadb_connector import MariaDBConnector


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
            table=self.file,
            control_value=self.control_value
        ).touch()

    def output(self):
        return self.get_luigi_target(
            table=self.file,
            control_value=self.control_value
        )

class ETLControlRegistration(LuigiMaridbTarget, luigi.Task):

    file = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = self.file
        self.control_value = f'{self.file} under id: '

    def run(self):
        """ register files.csv in etl_control table.
            data_load_id is returned as a control value for the file load"""

        sql_register = [f"insert into etl_control (file_name, status) values ('{self.file}', 'in progress')"]
        sql_get_id = [f"select data_load_id  from etl_control ec where file_name = '{self.file}'"]

        connector = MariaDBConnector()
        with connector:
            connector.execute(sql_register)

        with connector:
            self.control_value += str(connector.execute(sql_get_id)[0][0]) # [(int),] is a return value

        logger.info(f'{self.file} is registred in etl_control with data_load_id = {self.control_value}')

        super().run()



class LALoadTask(LuigiMaridbTarget, luigi.Task):
    """ingest data from csv file in db dynamicly relying on patterns"""

    file = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        self.file = self.file
        self.control_value = f"{self.file} under id: "
        self.la_table = f"la_{self.file.split('_')[0]}"

    def requires(self):
        return ETLControlRegistration(file=self.file)
    
    def run(self):
        file_landing_zone = config.get('path', 'file_landing_zone')
        file_path = os.path.join(file_landing_zone, self.file)

        with open(file_path,'r',encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file,delimiter='#')
            header = next(csv_reader)
            header.append('data_load_id')
            counter = 0
            db_connector = MariaDBConnector()
            with db_connector:
                for row in csv_reader:
                    insert_query = [f"""
                        INSERT INTO {self.la_table} ({', '.join(header)})
                        VALUES ({', '.join(f'"{element}"' for element in row)}, '{self.control_value}')
                    """]
                    db_connector.execute(insert_query)
                    counter += 1

        logger.info(f"{counter} rows were inserted into {self.la_table}")

        super().run()



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

    def requires(self):
        return TRLoadTask(file=self.file)


class AskSpotifyPipeline(LuigiMaridbTarget, luigi.Task):
    """pipeline reversed entry point."""

    files_to_process = luigi.ListParameter()


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = __class__.__name__
        self.control_value = '-'.join(sorted(self.files_to_process))

    def requires(self):
        for file in self.files_to_process:
            # yield DSLoadTask(file=file) TODO: revert me back
            # yield ETLControlRegistration(file=file)
            yield LALoadTask(file=file)

            

"""ask_spotify main ETL pipeline implemented with Luigi"""
import csv
import logging
import os
import hashlib
import luigi

from luigi.contrib.mysqldb import MySqlTarget
from ask_spotify_etl_config import Config
from db_connector.mariadb_connector import MariaDBConnector
from sql_scripts.get_sql_script import get_sql_script, get_truncate


project_root = os.path.dirname(os.path.abspath(__file__))
logging.config.fileConfig(fname=os.path.join(project_root, 'config', 'logging.conf'))
logger = logging.getLogger()
config = Config()


class LuigiMaridbTarget():
    """ Parent class for all ETL Luigi tasks of the pipeline. 
        - Must be enherited by all tasks classes and RUN to be executed (super().run())
        - Takes care of Luigi tasks status control on Database.
        - Database control table: table_updates.
    """

    pipeline_files_control_value = ''

    def get_luigi_target(self, table, control_value):
        return MySqlTarget(host=config.get('database.credentials', 'host'),
                    database=config.get('database.credentials', 'database'),
                    user=config.get('database.credentials', 'user'),
                    password=config.get('database.credentials', 'password'),
                    table=table,
                    update_id=control_value)

    def run(self):
        return self.get_luigi_target(
            table=self.table,
            control_value=self.control_value
        ).touch()

    def output(self):
        return self.get_luigi_target(
            table=self.table,
            control_value=self.control_value
        )

    def get_data_load_id(self, file):
        sql_get_id = [f"select data_load_id from etl_control ec where file_name = '{file}'"]
        connector = MariaDBConnector()
        with connector:
            data_load_id = str(connector.execute(sql_get_id)[0][0]) # [(int),] is a return value
        return data_load_id



class ETLControlRegistration(LuigiMaridbTarget, luigi.Task):
    """ Beginning of the data load.
        Registration of file and generation of data_load_id.
        Run method: Data_load_id is returned as a control value for the file load.
    """

    file = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = self.file
        self.control_value = f'{__class__.__name__}:{self.file}:'
        self.table = 'etl_control'


    def run(self):
        sql_register = [f"insert into etl_control (file_name, status) values ('{self.file}', 'in progress')"]

        connector = MariaDBConnector()
        with connector:
            connector.execute(sql_register)

        self.control_value += self.get_data_load_id(self.file)

        logger.info(f'{self.file} is registred in etl_control with data_load_id = {self.control_value}')

        super().run()



class LALoadTask(LuigiMaridbTarget, luigi.Task):
    """ Data load: csv >> la_table
        - Ingest data from csv file in db dynamicly relying on patterns"""

    file = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = self.file
        self.control_value = f'{__class__.__name__}:{self.file}:'
        self.table = f"la_{self.file.split('_')[0]}"


    def requires(self):
        return ETLControlRegistration(file=self.file)

    def run(self):
        """data ingest csv >> db tables"""

        file_landing_zone = config.get('path', 'file_landing_zone')
        file_path = os.path.join(file_landing_zone, self.file)
        data_load_id = self.get_data_load_id(self.file)
        self.control_value += data_load_id

        with open(file_path,'r',encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file,delimiter='#')
            header = next(csv_reader)
            header.append('data_load_id')
            counter = 0
            db_connector = MariaDBConnector()
            with db_connector:
                for row in csv_reader:
                    # wrap each element in double quotes.
                    # in case double quotes exist in data, double it (escaping) for sql insert.
                    row_values = ', '.join(f'''"{element.replace('"', '""')}"''' for element in row)
                    insert_query = [f"""
                        INSERT INTO {self.table} ({', '.join(header)})
                        VALUES ({row_values}, '{data_load_id}')
                    """]
                    db_connector.execute(insert_query)
                    counter += 1
        logger.info(f"{counter} rows were inserted into {self.table}")


        super().run()


class LACompleteGateway(LuigiMaridbTarget, luigi.Task):
    """ Gateway task.
        - Make sure that all LA tasks are done, before TR load.
            - reason: FK dependencies
        - TRUNCATE tr_tables as a preparation before TR load.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = __class__.__name__
        self.control_value = self.table + \
                            ':' + \
                            hashlib.md5(LuigiMaridbTarget.pipeline_files_control_value.encode('utf-8')).hexdigest()

    def requires(self):
        return LuigiMaridbTarget.all_LA_dependent_tasks

    def run(self):
        """truncate TR layer before starting TR load"""

        for file in LuigiMaridbTarget.pipeline_files_control_value.split('-'):
            table = [file.split('_')[0]]
            truncate_sql = get_truncate(layer='tr', file=file)
            if table[0] == 'artists':
                truncate_sql += get_truncate(layer='tr', split_table='genres')
                truncate_sql += get_truncate(layer='tr', split_table='artists_genres')
                table.append('genres')
                table.append('artists_genres')

            db_connector = MariaDBConnector()
            with db_connector:
                db_connector.execute(truncate_sql)
                logger.info(f"truncated tables on TR layer: {table}")

        super().run()

class TRLoadTask(LuigiMaridbTarget, luigi.Task):
    """ Data load: la >> tr.
        type casting, data split, generation of surrogate keys.
        incremental load.
        """

    file = luigi.Parameter()
    LuigiMaridbTarget.all_LA_dependent_tasks = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = self.file
        self.control_value = f'{__class__.__name__}:{self.file}:'
        self.table = f"tr_{self.file.split('_')[0]}"
        LuigiMaridbTarget.all_LA_dependent_tasks.append(LALoadTask(file=self.file))

    def requires(self):
        return LACompleteGateway()

    @property
    def priority(self):
        """control on the order of execution of available tasks"""
        if self.file.split('_')[0] == 'artists':
            return 100
        elif self.file.split('_')[0] == 'albums':
            return 90
        elif self.file.split('_')[0] == 'tracks':
            return 80

    def run(self):
        data_load_id = self.get_data_load_id(self.file)
        self.control_value += data_load_id

        sql_script = get_sql_script(layer='tr',file=self.file,data_load_id_param=data_load_id)

        # split logic for data normalization
        if self.file.split('_')[0] == 'artists':
            sql_script += get_sql_script(layer='tr', split_table='genres',data_load_id_param=data_load_id)
            sql_script += get_sql_script(layer='tr', split_table='artists_genres',data_load_id_param=data_load_id)


        db_connector = MariaDBConnector()
        with db_connector:
            db_connector.execute(sql_script)

        logger.info(f"tr_{self.table} was loaded under id: {data_load_id}")

        super().run()


class DSLoadTask(LuigiMaridbTarget, luigi.Task):
    """ Data load: tr >> ds.
        INSERT for new data.
        UPDATE for existing data.
        No history is build.
    """

    file = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = self.file
        self.control_value = f'{__class__.__name__}:{self.file}:'
        self.table = f"ds_{self.file.split('_')[0]}"

    def requires(self):
        return TRLoadTask(file=self.file)


    def run(self):
        data_load_id = self.get_data_load_id(self.file)
        self.control_value += data_load_id

        sql_script = get_sql_script(layer='ds',file=self.file)

        # split logic for data normalization
        if self.file.split('_')[0] == 'artists':
            sql_script += get_sql_script(layer='ds', split_table='genres')
            sql_script += get_sql_script(layer='ds', split_table='artists_genres')

        db_connector = MariaDBConnector()
        with db_connector:
            db_connector.execute(sql_script)

        logger.info(f"ds_{self.table} was loaded under id: {data_load_id}")

        super().run()


class AskSpotifyPipeline(LuigiMaridbTarget, luigi.Task):
    """ Pipeline reversed entry point.
        - executed as a last task of the pipeline.
        - takes care of file load unregistration in etl_control table.
    """
    files_to_process: list[str] = luigi.ListParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # define pipeline level control postfix
        LuigiMaridbTarget.pipeline_files_control_value = '-'.join(sorted(self.files_to_process))
        self.table = __class__.__name__
        self.control_value = self.table + LuigiMaridbTarget.pipeline_files_control_value

    def requires(self):
        for file in self.files_to_process:
            yield DSLoadTask(file=file)

    def run(self):
        # unregister files loaded by pipeline
        sql = """
            UPDATE etl_control
            SET status = 'finished'
            WHERE file_name = '{file_name}'
            AND data_load_id = {data_load_id}
            """
        for file in self.files_to_process:
            data_load_id = self.get_data_load_id(file)
            connector = MariaDBConnector()
            with connector:
                connector.execute([sql.format(file_name=file,data_load_id=data_load_id)])
            logger.info(f"{file} was unregistred with status finished under id:{data_load_id}")

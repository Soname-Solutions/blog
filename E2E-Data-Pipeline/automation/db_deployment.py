"""
The automation module to generate scripts for DB objects.
The module allows DROP, CREATE of the objects on database passing corresponding options.
"""

import argparse
import os
import sys

import mariadb
from dotenv import load_dotenv

load_dotenv()

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')


# prepare PATH values for scripts generation
root_file_path = os.path.dirname(__file__)
drop_file = os.path.join(root_file_path, 'tmp', 'drop_table.sql')
db_objects_folder = os.path.join(os.path.abspath(os.path.join(root_file_path, os.pardir)),
                                 'db_objects')


def create_drop_file(output_file):
    """create empty file with DDL DROP statements"""

    with open(output_file, 'w+', encoding='UTF-8'):
        pass


def create_db_connection():
    """create MariaDB connection"""

    try:
        conn = mariadb.connect(
            host="localhost",
            port=3306,
            user=db_user,
            password=db_password,
            database=db_name)
    except mariadb.Error as error:
        print(f"Error connecting to the database: {error}")
        sys.exit(1)

    return conn


def execute_sql(ddl):
    """deploy generated DDL scripts if deployment parameter is True"""

    conn = create_db_connection()
    cur = conn.cursor()
    select_output = list()
    for sql in ddl:
        try:
            cur.execute(sql)

            if sql.upper().strip().startswith('SELECT'):
                select_output.append(list(cur))

        except mariadb.Error as error:
            print(f"DDL exectuion error: {error}")
            conn.close()
            sys.exit(1)

    conn.close()
    return select_output


def drop_fk_contraints(output_drop_file, drop_objects_param):
    """ select all FK constraints.
        generate drop FK constraint statement.
        execute drop FK constraint statement."""

    select_fk_constraints_sql = f"""
    SELECT
        TABLE_NAME, CONSTRAINT_NAME 
    FROM
        information_schema.TABLE_CONSTRAINTS
    WHERE 1=1
        AND CONSTRAINT_TYPE = 'FOREIGN KEY'
        AND lower(TABLE_SCHEMA) = lower('{db_name}')
    """
    # expected data structure:
    #   [[('ds_artists_genres', 'ds_artists_genres_ibfk_1'),
    #   ('ds_artists_genres', 'ds_artists_genres_ibfk_2')]]
    selected_fk = execute_sql([select_fk_constraints_sql])

    for db_constrain in selected_fk[0]:
        table_name, fk_name = db_constrain
        drop_fk_sql = f'ALTER TABLE {table_name} DROP FOREIGN KEY {fk_name}; \n'

        with open(output_drop_file, 'a', encoding='UTF-8') as write_file:
            write_file.write(drop_fk_sql)
        if drop_objects_param:
            execute_sql([drop_fk_sql])
            print(f'FK {fk_name} is dropped ...')


def get_deployment_tables(file_paths):
    """get all table names from db deployment scripts"""

    table_names = []

    for file in file_paths:
        with open(file, 'r', encoding='UTF-8') as read_file:
            for line in read_file:
                # find table names as a substring of CREATE statement
                if line.strip().startswith("CREATE TABLE"):
                    table_names.append(line[line.find('`')+1:line.rfind('`')])

    return table_names


def drop_objects(file_paths, output_drop_file, drop_objects_param):
    """generate DROP statements. Execute DROP statements if drop_objects_param"""

    tables = get_deployment_tables(file_paths)
    drop_tables_sql = [f'DROP TABLE IF EXISTS {table_name};' for table_name in tables]

    if drop_objects_param:
        execute_sql(drop_tables_sql)
        for table in tables:
            print(f'{table} was dropped ...')

    with open(output_drop_file, 'a', encoding='UTF-8') as write_file:
        for sql in drop_tables_sql:
            write_file.write(sql + '\n')


def create_objects(file_paths, deploy_objects_param):
    """generate CREATE statements. Execute CREATE statements if deploy_objects_param"""

    create_statement = ''
    create_sql = []

    for file in file_paths:
        with open(file, 'r', encoding='UTF-8') as read_file:
            lines = read_file.readlines()

        for line in lines:
            create_statement += line
            if line.endswith(';\n'):
                create_sql.append(create_statement)
                create_statement = ''

        if deploy_objects_param:
            execute_sql(create_sql)
            print(f'{file} was deployed ...')


def db_deployment(  db_objects,
                    output_drop_file,
                    drop_objects_param=False,
                    deploy_objects_param=False):
    """Module entry point.
    - Generate file with ALTER TABLE {table_name} DROP FOREIGN KEY {fk_name}.
    - Generate file with DROP statements for all objects in db_objects folder.
    - Generate file with CREATE statements for all objects in db_objects folder.
    """

    # creaate fresh deploy files
    create_drop_file(output_drop_file)
    drop_fk_contraints(output_drop_file, drop_objects_param)

    for root, _, files in os.walk(db_objects):
        if len(files) != 0 and not root.endswith('dbml'):
            file_paths = [os.path.join(root, file) for file in files]

            drop_objects(file_paths, output_drop_file, drop_objects_param)
            create_objects(file_paths, deploy_objects_param)



if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="Automation module to generate DB objects"
    )

    parser.add_argument("-d",
                        "--drop",
                        action="store_true",
                        help="executes generated DROP statements on DB")
    parser.add_argument("-c",
                        "--create",
                        action="store_true",
                        help="executes CREATE statements on DB")

    args = parser.parse_args()

    db_deployment(  db_objects_folder,
                    drop_file,
                    drop_objects_param=args.drop,
                    deploy_objects_param=args.create)

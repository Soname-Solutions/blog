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
file_path = os.path.dirname(__file__)
deploy_file = os.path.join(file_path, 'tmp', 'db_deployment.sql')
drop_file = os.path.join(file_path, 'tmp', 'drpo_table.sql')
db_objects_folder = os.path.join(os.path.abspath(os.path.join(file_path, os.pardir)), 'db_objects')
# db objects dependency dict (proper order needed for deployment)
ordered_dickt = {'artists' : 1, 'albums' : 2, 'tracks' : 3,
                 'genres' : 4, 'artists_genres': 5, 'control': 6}


def create_deploy_file(output_file):
    """create empty file with DDL CREATE statements"""

    with open(output_file, 'w+', encoding='UTF-8'):
        pass


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


def deploy_db_objects(ddl):
    """deploy generated DDL scripts if deployment parameter is True"""

    conn = create_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(ddl)
    except mariadb.Error as error:
        print(f"DDL exectuion error: {error}")
        conn.close()
        sys.exit(1)

    conn.close()


def generate_drop_statements(files_list, output_drop_file, drop_objects):
    """generate DROP statements. Execute DROP statements if drop_objects"""
    for file_name in files_list:
        print(f'write DROP for {file_name} ...')
        drop_statement = f'DROP TABLE {file_name[ : len(file_name) - 4]}; \n'

        with open(output_drop_file, 'a', encoding='UTF-8') as write_file:
            write_file.write(drop_statement)

        if drop_objects:
            print(f'Execute drop statement for: {file_name[ : len(file_name) - 4]}')
            deploy_db_objects(drop_statement)


def generate_create_statements(files_list, output_deploy_file, deploy_objects):
    """generate CREATE statements. Execute CREATE statements if deploy_objects"""

    for file_name in files_list:
        print(f'write {file_name} into deploy file ...')


        with open(file_name, 'r', encoding='UTF-8') as read_file:
            create_table_statement = read_file.read()

            with open(output_deploy_file, 'a', encoding='UTF-8') as file:
                file.write('\n')
                file.writelines(create_table_statement)
                file.write('\n')

            if deploy_objects:
                print(f'Execute create statement for: {file_name}')
                deploy_db_objects(create_table_statement)


def generate_deployment(db_objects,
                        output_deploy_file,
                        output_drop_file,
                        drop_objects=False,
                        deploy_objects=False):
    """Module entry point.
    - Generate files with DROP statements for all objects in db_objects folder.
    - Generate files with CREATE statements for all objects in db_objects folder.
    """

    # creaate fresh deploy files
    create_deploy_file(output_deploy_file)
    create_drop_file(output_drop_file)

    for root, _, files in os.walk(db_objects):

        if len(files) != 0:
            sorted_files = sorted(files, key=lambda d: ordered_dickt[d[d.index('_')+1 : len(d)-4]])
            sorted_file_paths = [os.path.join(root, file) for file in sorted_files]
            sorted_files_reversed = sorted_files[::-1]

            generate_drop_statements(sorted_files_reversed, output_drop_file, drop_objects)
            generate_create_statements(sorted_file_paths, output_deploy_file, deploy_objects)



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
                        help="executes generated CREATE statements on DB")

    args = parser.parse_args()

    generate_deployment(db_objects_folder,
                        deploy_file,
                        drop_file,
                        drop_objects=args.drop,
                        deploy_objects=args.create)

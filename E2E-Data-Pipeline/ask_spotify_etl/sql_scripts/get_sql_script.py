"""utility functions to generate SQL scripts"""

import os

def get_sql_script(layer: str, file: str = None, split_table: str = None, data_load_id_param: int = None) -> list[str]:
    """function to return prepared sql script from file"""
    if file:
        entity = file.split('_')[0]
    else:
        entity = split_table
    scripts_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), layer, f'{entity}.sql')

    with open(scripts_file,'r',encoding='utf-8') as sql_file:
        sql_scripts = sql_file.read()

    sql_list = [sql.format(data_load_id = str(data_load_id_param)) for sql in sql_scripts.split(';') if sql != '\n']

    return sql_list


def get_truncate(layer: str, file: str = None, split_table: str = None) -> None:
    """dynamic constructor for truncate SQL"""
    sql = "truncate table {table_name}"

    if file:
        table_name = f"{layer}_{file.split('_')[0]}"
    else:
        table_name = f"{layer}_{split_table}"

    return [sql.format(table_name=table_name)]



if __name__ == "__main__":
    print(get_sql_script(layer='tr',
                         file='albums_2023_08_12.csv',
                         data_load_id_param=1
                         ))

    print(get_truncate(layer='tr',
                       file='albums_2023_08_12.csv'))

    print(get_truncate(layer='tr',
                       split_table='genres'))

import os

def get_sql_script(layer: str, file: str) -> str:
    """function to return prepared sql script from file"""

    entity = file.split('_')[0]
    scripts_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), layer, f'{entity}.sql')

    with open(scripts_file,'r',encoding='utf-8') as sql_file:
        sql_scripts = sql_file.read()

    return sql_scripts


if __name__ == "__main__":
    print(
        get_sql_script('tr', 'artists_2023_08_12.csv') % ('1', '1')
        )
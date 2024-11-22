import json

def import_schema(path):
    with open(path, 'r') as file:
        return json.load(file)
    
def table_exists(database, table, spark):
    count = spark.sql(f'show tables in {database}').filter(f'tableName = "{table}"').count()
    return count == 1

def import_query(path):
    with open(path, 'r') as file:
        query = file.read()
    return query
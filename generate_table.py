import yaml
import psycopg2
from config import get_config

def create_table_from_yaml(yaml_file, table_name):
    with open(yaml_file, 'r') as file:
        schema = yaml.safe_load(file)
    columns = schema["columns"]
    columns_sorted = sorted(columns, key=lambda col: col['position'])
    column_definitions = []
    for column in columns_sorted:
        col_def = f"{column['name']} {column['type']}"
        if not column['nullable']:
            col_def += " NOT NULL"
        if column['default']:
            col_def += f" DEFAULT {column['default']}"
        column_definitions.append(col_def)

    create_table_sql = "CREATE TABLE " + table_name + " (\n  " + ",\n  ".join(column_definitions) + "\n);"

    try:
        conn = psycopg2.connect(**get_config("database.ini", "postgresql_aws"))
        cursor = conn.cursor()

        cursor.execute(create_table_sql)
        
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":

    table_name = "customers"
    yaml_file = "schema.yaml"
    try:
        create_table_from_yaml(yaml_file, table_name)
    except Exception as e:
        print(f"Error: {e}")
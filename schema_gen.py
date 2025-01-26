import yaml
import psycopg2
from config import get_config

def get_table_schema_pg(table_name):
    """
    Fetch table schema for the given table name from a PostgreSQL database.
    """
    conn = psycopg2.connect(**get_config("database.ini", "postgresql"))
    cursor = conn.cursor()
    
    # Query to fetch column details
    cursor.execute(f"""SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
    """)
    columns = cursor.fetchall()
    
    schema = []
    for column in columns:
        col_name, col_type, is_nullable, col_default, ordinal_position = column
        column_schema = {
            "name": col_name,
            "type": col_type,
            "nullable": is_nullable == "YES",
            "default": col_default if col_default is not None else None,
            "position": ordinal_position
        }
        schema.append(column_schema)
    
    cursor.close()
    conn.close()
    return schema


def generate_schema_yaml(schema, output_file):
    """
    Generate a schema.yaml file from the provided schema.
    """
    schema_yaml = {"columns": schema}
    
    with open(output_file, 'w') as file:
        yaml.dump(schema_yaml, file, default_flow_style=False)
    
    print(f"Schema YAML file saved to {output_file}")

if __name__ == "__main__":

    table_name = "customers"
    yaml_file = "schema.yaml"
    
    # Get schema and generate schema.yaml
    try:
        schema = get_table_schema_pg(table_name)
        generate_schema_yaml(schema, yaml_file)
    except Exception as e:
        print(f"Error: {e}")

from configparser import ConfigParser

def get_config(file_name,section):
    file_loc=fr"C:/Users/Lenovo/Desktop/miscellenious_problems-main/Schema/{file_name}"
    parser = ConfigParser()
    parser.read(file_loc)
    db_localhost = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_localhost[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in {file_name}")
    return db_localhost

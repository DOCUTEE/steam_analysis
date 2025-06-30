from string import Template

def read_sql_file(file_path, **kwargs):
    """
    Reads an SQL file and substitutes any placeholders with provided keyword arguments.
    
    :param file_path: Path to the SQL file.
    :param kwargs: Keyword arguments for substitution in the SQL template.
    :return: The SQL content with substitutions applied.
    """
    try:
        with open(file_path, 'r') as file:
            sql_content = Template(file.read())
            return sql_content.substitute(**kwargs)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"SQL file not found at: {file_path}") from e
    except Exception as e:
        raise Exception(f"Error reading or processing SQL file at: {file_path}") from e

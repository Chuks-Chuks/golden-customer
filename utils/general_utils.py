import os
import yaml

def ensure_directory_exists(directory: str) -> None:
    """
    Ensure that the specified directory exists, creating it if necessary.
    Args:
        directory (str): The path of the directory to check or create.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

def read_yaml_config(config_path: str) -> dict:
    """
    Read a YAML configuration file and return its contents as a dictionary.
    Args:
        config_path (str): The path to the YAML configuration file.
    Returns:
        dict: The contents of the YAML file as a dictionary.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config
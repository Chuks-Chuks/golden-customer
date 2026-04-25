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
    # Ensure the configuration file exists when reading
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_config_path = os.path.join(project_root, config_path)
    if not os.path.exists(full_config_path):
        raise FileNotFoundError(f"Configuration file not found: {full_config_path}")
    with open(full_config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config
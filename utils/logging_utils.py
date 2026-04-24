import logging
import os
from utils.general_utils import ensure_directory_exists as ede

def get_logger(config) -> logging.Logger:
    """
    Set up logging configuration for the pipeline.
    Args:        config (dict): The configuration dictionary.
    Returns:     logging.Logger: Configured logger instance.
    """
    log_dir = config["logging"].get("log_dir", "logs/")
    log_file = config["logging"].get("log_file", "pipeline.log")
    log_level = config["logging"].get("level", "INFO").upper()
    log_format = config["logging"].get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Ensure the log directory exists
    ede(log_dir)
    log_path = os.path.join(log_dir, log_file)

    logger = logging.getLogger("GoldenRecordPipeline")

    if logger.hasHandlers():
        return logger
    
    logger.setLevel(log_level)

    formatter = logging.Formatter(log_format)

    # Logging for console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Logging for file
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
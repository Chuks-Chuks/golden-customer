import logging

def logger_setup(level=logging.INFO) -> logging.Logger:
    """
    Set up logging configuration for the pipeline.
    Args:        level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
    Returns:     logging.Logger: Configured logger instance.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("pipeline.log")
        ]
    )
    return logging.getLogger("GoldenRecordPipeline")
import logging

def get_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)  # so we see logged messages in console when debugging    
    return logger

def logger_add_file_handler(logger, log_name):    
    file_name = f"log/{log_name}.log"
    file_handler = logging.FileHandler(file_name)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    file_handler.setFormatter(formatter)    
    logger.addHandler(file_handler)
    print(f"Added file handler {file_name}")
   
def logger_remove_file_handler(logger, log_name):
    file_name = f"log/{log_name}.log"
    file_handler = logging.FileHandler(file_name)

    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler) and handler.baseFilename == file_handler.baseFilename:
            logger.removeHandler(handler)
            print(f"Removed file handler {file_name}")


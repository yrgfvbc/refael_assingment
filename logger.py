import logging


# Generic logging code
def make_logger(logger_name, log_file_name="refael_assignment.log", display_name=None):

    if display_name is None:
        logging_format = logging.Formatter('%(asctime)s - %(name)s: %(message)s')
    else:
        logging_format = logging.Formatter('%(asctime)s - ' + display_name + ' : %(message)s')

    logger = logging.getLogger(logger_name)
    logger.setLevel(level=logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging_format)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file_name)
    file_handler.setLevel(level=logging.DEBUG)
    file_handler.setFormatter(logging_format)
    logger.addHandler(file_handler)

    return logger

import logging
from logging.handlers import TimedRotatingFileHandler


def get_logger(filepath='logs.log', name=__name__):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        # create a console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # create a file handler
        file_handler = TimedRotatingFileHandler(
            filepath, when='midnight', interval=1, backupCount=7, encoding='utf-8')
        file_handler.setLevel(logging.INFO)

        # create a formatter and add to handlers
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # add handlers to logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger

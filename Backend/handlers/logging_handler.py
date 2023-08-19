import logging
from datetime import date
import os

filename = f'log-{str(date.today())}'

if not os.path.isfile(f'Backend//logs/{filename}'):
    os.makedirs(f"{os.getcwd()}\\backend\\logs\\{filename}",exist_ok=True)

'''
logging handler

used by other handlers to log progress and errors,

in its separate file to prevent circular import errors

'''

file_handler = logging.FileHandler(f'Backend/logs/{filename}/logs.log')
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s %(message)s'))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

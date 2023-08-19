import pandas as pd
from Backend.handlers.logging_handler import logger
import os

'''
Reset handler 

In case of a full reset, delete tables and clear previously acquired data and history

'''


def FullReset(handler):
    pd.read_csv(
        'Backend/Data/History.csv').iloc[0:0].to_csv('Backend/Data/History.csv', index=False)
    pd.read_csv(
        'Backend/Data/ProductAnalysis.csv').iloc[0:0].to_csv('Backend/Data/ProductAnalysis.csv', index=False)
    try:
        for file in os.listdir('./Backend/Data/'):
            if file != 'History.csv' and file != 'ProductAnalysis.csv':
                os.remove(f'Backend/Data/{file}')
    except:
        logger.info('no file to remove')
    try:
        for file in os.listdir('./Backend/RedditData'):
            os.remove(f'Backend/RedditData/{file}')
    except:
        logger.info('no file to remove')
    try:
        for file in os.listdir('./Backend/RedditData'):
            os.remove(f'Backend/RedditData/{file}')
    except:
        logger.info('no file to remove')
    
    try:
        for dir in os.listdir('./MigrationScripts/figures'):
            for file in os.listdir(f'./MigrationScripts/figures/{dir}'):
                os.remove(f'./MigrationScripts/figures/{dir}/{file}')
            os.removedirs(f'./MigrationScripts/figures/{dir}')
        os.mkdir('./MigrationScripts/figures')
    except:
        logger.info('no file to remove')
        
    logger.info("Deleted all tables and reset History")
    handler.deleteAllTbl()

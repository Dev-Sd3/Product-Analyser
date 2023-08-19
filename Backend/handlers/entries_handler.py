from Backend.handlers.logging_handler import logger
import pdb

def addEntries(fullRefresh,products=[]):
    logger.info("Adding Entries")
    mode = 'a'
    if fullRefresh:
        mode = 'w'
    with open("Backend/Data/entries.txt",mode) as f:
        for product in products:
            f.write(f'{product},')
            
def getEntries():
    logger.info("Getting Entries")
    with open("Backend/Data/entries.txt",'r') as f:
        content = f.readline().split(',')[:-1]
        return (content)
    
        
        
from Backend.handlers.bestbuy_handler import BestBuy
from Backend.handlers.logging_handler import logger
from Backend.handlers.analysis_handler import ProductAnalyser
from Backend.handlers.database_handler import PostGRE
from Backend.handlers.reddit_handler import RedditHandler
from Backend.handlers.sentiment_handler import SentimentAnalysis
from Backend.handlers.reset_handler import FullReset
from Backend.handlers.credentials_handler import getCredentials
from Backend.handlers.bestbuy_handler import MultiThreadedAcquisition
from Backend.handlers.backup_handler import BackupHandler
from Backend.handlers.trends_handler import GoogleTrends
from Backend.handlers.entries_handler import *
from MigrationScripts.sql import Migrations


'''
Before running code:

> Make sure to have a postgres database with the proper names

> Make sure the Credentials folder contains credentials.json / url.txt / key.key / encrypted.key  otherwise assertions will fail

> if you are on mac, change line 49 of bestbuy_handler to  -> self.driver = webdriver.Chrome('Driver/chromedriver')

---------------------------------------------

This is the m change the products array below according to the products you want analysed, and specify the status of fullRefresh

'''


def interface(products,fullRefresh,append=False):
    
    try:
        redditCredentials, postGreCredentials, url = getCredentials()
        backupHandler = BackupHandler(url)
        handler = PostGRE(
            user=postGreCredentials['user'],
            password=postGreCredentials['password'],
            host=postGreCredentials['host'],
            name=postGreCredentials['name']
        )
        
    except Exception as e:
        logger.fatal("Couln't get credentials")
        raise e
    
    try:
        if fullRefresh:
            FullReset(handler)
            
        if append or fullRefresh:
            addEntries(fullRefresh,products)
        
        all_products = getEntries()
            
        logger.info(f"Starting Operation with products: {products}")

        ################################################
        
        stgBestbuyReviews = MultiThreadedAcquisition(products,fullRefresh,append)

        ################################################

        analyser = ProductAnalyser(frame=stgBestbuyReviews,url=url,fullRefresh=fullRefresh)
        stgReviewAnalysis = analyser.analyseProduct()
        
        ################################################
        
        trends = GoogleTrends(all_products)
        stgGoogleTrends = trends.writeFrame(stgBestbuyReviews['PK_Md'].tolist())

        ###############################################
        
        
        reddit = RedditHandler(
            cid=redditCredentials['cli_id'],
            sec=redditCredentials['secret'],
            usr=redditCredentials['username'],
            psw=redditCredentials['passw'],
            agt=redditCredentials['user_agent'],
            products=all_products,
            limit=redditCredentials['limit']
        )

        stgRedditPosts = reddit.MultiThreadedAcquireData(fullRefresh)

        ################################################

        handler.updateAll(stgBestbuyReviews,stgRedditPosts,stgReviewAnalysis,stgGoogleTrends,fullRefresh)
        
        # sentimentAnalyser = SentimentAnalysis(url)
        # sentimentAnalyser.writeToPostGre()
        
        Migrator = Migrations(url)
        Migrator.runAll()
        
        
        backupHandler.getBackup()
        logger.info("Completed Operation")
        return True
    
    except Exception as e:
        logger.error(e)
        logger.warning("restoring database to previous state")
        backupHandler.restoreBackup()
        Migrator = Migrations(url)
        Migrator.runAll()
        return False




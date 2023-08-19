import pandas as pd
import hashlib
from Backend.handlers.logging_handler import logger
from sqlalchemy import create_engine


'''

This is the analysis handler which analyses each product dataframe found in the Data folder and finds
relevant attributes, each row contains: [PK_Md,product,startDate,endDate,no_reviews,avg_rating,recommendation_rate,nps]

After products are analysed, they are placed into the ProductAnalysis.csv file

'''


class ProductAnalyser:
    def __init__(self,frame,url,fullRefresh):
        
        if fullRefresh:
            self.frame = frame
            self.products = self.frame["product"].unique().tolist()
        else:
            with create_engine(url).connect() as conn:
                df = pd.read_sql("""
                                  SELECT * FROM "ProductAnalysis"."stgBestbuyReviews"
                                 """,conn)
                self.frame = pd.concat([df,frame]).drop_duplicates()
                self.products = self.frame["product"].unique().tolist()
            

    def analyseProduct(self):
        analysis = []
        for product in self.products:
            data = self.frame.loc[self.frame["product"] == product]
            PK_Md = hashlib.md5(product.encode()).hexdigest()
            startDate = data['reviewDate'].tolist()[0]
            endDate = data['reviewDate'].tolist()[len(data)-1]
            no_reviews = len(data)
            avg_rating = data['rating'].mean()
            recommendation_rate = len(data[data['recommendation']])/no_reviews
            nps = ((len(data[data['rating'] > 3]) -
                   len(data[data['rating'] < 3])) / no_reviews) * 100
            analysis.append([PK_Md, product, startDate, endDate, no_reviews, avg_rating, recommendation_rate, nps])
        df =  pd.DataFrame(analysis,columns=['PK_Md', 'product', 'endDate','startDate', 'Reviews', 'avgRating', 'recommendationRate', 'nps'])
        df.to_csv("Backend/Data/ProductAnalysis.csv",index=False)
        logger.info("Analysed data from BestBuy")
        return df
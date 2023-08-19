import pdb
import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
from Backend.handlers.logging_handler import logger

'''
Database handler

this file is the portion responsible for exporting locally found dataframes to the postGres server

It works by fusing separate dataframes found in the Data and RedditData files, as well as the History and ProductAnalysis file 
then exporting the 4 stages to postgres, then enforcing the relationship between them

works both in fullRefresh and normal refresh modes

'''


class PostGRE:
    def __init__(self, user, password, host, name):
        self.conn = psycopg2.connect(
            host=host,
            dbname=name,
            user=user,
            password=password,
            port=5432
        )
        
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.connection = f'postgresql://{user}:{password}@{host}/{name}'

    def copyIntoTable(self, df, table_name, fullRefresh):
        db = create_engine(self.connection)
        conn = db.connect()
        if fullRefresh:
            df.to_sql(table_name, conn, if_exists='replace',
                      index=False, schema='ProductAnalysis')
        else:
            sql = pd.read_sql(
                f'SELECT * FROM  "ProductAnalysis"."{table_name}"', conn)
            if table_name == "stgBestbuyReviews" or table_name == "stgRedditPosts":
                diff = df[~df['reviewId'].isin(sql['reviewId'])]
                diff.to_sql(table_name, conn, if_exists='append',
                            index=False, schema='ProductAnalysis')
                return

            elif table_name == "fctHistory":
                diff = df[~df['Version'].isin(sql['Version'])]
                with db.connect() as engine:
                    for pk, version, product,url,date in zip(diff['PK_Md'], diff['Version'], diff['Product'],diff['url'],diff['date']):
                        engine.execute(f"""
                            INSERT INTO "ProductAnalysis"."fctHistory" ("PK_Md", "Version", "Product", "url", "date")
                            VALUES ('{pk}', '{version}', '{product}', '{url}', '{date}')
                            ON CONFLICT ("PK_Md") DO UPDATE
                            SET "Version" = EXCLUDED."Version", "Product" = EXCLUDED."Product", "url" = EXCLUDED."url", "date" = EXCLUDED."date"
                                       """)

            elif table_name == "aggReviewAnalysis":
                df.to_sql(table_name, conn, if_exists='replace',
                          index=False, schema='ProductAnalysis')
                return
        
            elif table_name == "stgGoogleTrends":
                fused = pd.merge([sql,df])
                fused.to_sql(table_name, conn, if_exists='append',index=False, schema='ProductAnalysis')     

    def setRelationships(self):
        db = create_engine(self.connection)
        with db.connect() as conn:
            conn.execute("""
            ALTER TABLE "ProductAnalysis"."fctHistory"
            ADD PRIMARY KEY ("PK_Md"),
            ADD CONSTRAINT unique_pk_md UNIQUE ("PK_Md")
            """)

            conn.execute("""
                ALTER TABLE "ProductAnalysis"."stgBestbuyReviews"
                ADD CONSTRAINT fk_pkmd
                FOREIGN KEY ("PK_Md")
                REFERENCES "ProductAnalysis"."fctHistory" ("PK_Md")
                ON DELETE CASCADE
            """)

            conn.execute("""
                ALTER TABLE "ProductAnalysis"."stgRedditPosts"
                ADD CONSTRAINT fk_pkmd
                FOREIGN KEY ("PK_Md")
                REFERENCES "ProductAnalysis"."fctHistory" ("PK_Md")
                ON DELETE CASCADE
                """)

            conn.execute("""
                ALTER TABLE "ProductAnalysis"."aggReviewAnalysis"
                ADD CONSTRAINT fk_pkmd
                FOREIGN KEY ("PK_Md")
                REFERENCES "ProductAnalysis"."fctHistory" ("PK_Md")
                ON DELETE CASCADE
                """)

    def updateAll(self,bestbuy,reddit,analysis,trends,fullRefresh=True):

        with create_engine(self.connection).connect() as conn:
            df = pd.read_sql("""
                        SELECT table_name 
                        FROM information_schema.tables
                        WHERE table_schema = 'ProductAnalysis';
                            """, conn)
            found_tables = set(df['table_name'].tolist())
            necessary_tables = set(
                ['aggReviewAnalysis', 'fctHistory', 'stgBestbuyReviews', 'stgRedditPosts'])
            if len(found_tables & necessary_tables) < 4 and not fullRefresh:
                logger.warning(
                    "Cant insert data into tables in update mode since no data is present!, switching to fullRefresh mode")
                fullRefresh = True

        if fullRefresh:
            self.deleteAllTbl()
        
        history = pd.read_csv('Backend/Data/History.csv')
        
        self.copyIntoTable(history, 'fctHistory', fullRefresh)
        self.copyIntoTable(analysis, 'aggReviewAnalysis', fullRefresh)
        self.copyIntoTable(bestbuy, 'stgBestbuyReviews', fullRefresh)
        self.copyIntoTable(reddit, 'stgRedditPosts', fullRefresh)
        if type(trends) != bool:
            self.copyIntoTable(trends, 'stgGoogleTrends', fullRefresh=True)
        
        if fullRefresh:
            self.setRelationships()
            
        logger.info("Updated entire database")

    def deleteAllTbl(self):
        tbls = ['aggReviewAnalysis', 
                'stgBestbuyReviews',
                'fctHistory', 
                'stgRedditPosts', 
                'aggSentimentAnalysis',
                'stgGoogleTrends',
                'aggAvgRating',
                'aggCumulativeAvgNps',
                'aggCumulativeAvgRating',
                'aggCumulativeRecommendations',
                'aggDailyNps',
                'aggEntryCount',
                'aggRecommendations',
                ]
        for tbl in tbls:
            self.cur.execute(f"""DROP TABLE IF EXISTS "ProductAnalysis"."{tbl}" CASCADE
            """)
        logger.info("Dropped all tables")

    def executeSearchQuerry(self, query):
        self.cur.execute(query)
        return self.cur.fetchall()

    def __del__(self):
        try:
            self.cur.close()
            self.conn.close()
        except:
            pass

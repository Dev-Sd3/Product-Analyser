import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os

class BackupHandler:
    def __init__(self,url):
        self.url = url
        self.engine = create_engine(url)
        
    def getBackup(self):
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y-%H-%M-%S")
        os.mkdir(f'Backend/Backups/Backup-{dt_string}')
        with self.engine.connect() as conn:

            history = pd.read_sql("""
            SELECT * FROM "ProductAnalysis"."fctHistory"
            """,conn)

            analysis = pd.read_sql("""
            SELECT * FROM "ProductAnalysis"."aggReviewAnalysis"
            """,conn)

            reviews = pd.read_sql("""
            SELECT * FROM "ProductAnalysis"."stgBestbuyReviews"
            """,conn)

            reddit = pd.read_sql("""
            SELECT * FROM "ProductAnalysis"."stgRedditPosts"
            """,conn)
            
        history.to_csv(f'Backend/Backups/Backup-{dt_string}/history.csv',index=False)
        analysis.to_csv(f'Backend/Backups/Backup-{dt_string}/analysis.csv',index=False)
        reviews.to_csv(f'Backend/Backups/Backup-{dt_string}/reviews.csv',index=False)
        reddit.to_csv(f'Backend/Backups/Backup-{dt_string}/reddit.csv',index=False)
        
        with open('Backend/Backups/latest.txt','w') as ver:
            ver.write(f'Backup-{dt_string}')
        
        return dt_string
        
    def restoreBackup(self,version=""):
                      
        if version == "":
            with open('Backend/Backups/latest.txt','r') as ver:
                version = ver.read()
        
        
        history = pd.read_csv(f'Backend/Backups/{version}/history.csv')
        analysis = pd.read_csv(f'Backend/Backups/{version}/analysis.csv')
        reviews = pd.read_csv(f'Backend/Backups/{version}/reviews.csv')
        reddit = pd.read_csv(f'Backend/Backups/{version}/reddit.csv')
        
        history.to_csv('Backend/Data/History.csv',index=False)
        analysis.to_csv('Backend/Data/ProductAnalysis.csv',index=False)
        
        with self.engine.connect() as conn:
            conn.execute("""
                DROP TABLE IF EXISTS "ProductAnalysis"."fctHistory" CASCADE;
                DROP TABLE IF EXISTS "ProductAnalysis"."aggReviewAnalysis" CASCADE;
                DROP TABLE IF EXISTS "ProductAnalysis"."stgRedditPosts" CASCADE;
                DROP TABLE IF EXISTS "ProductAnalysis"."stgBestbuyReviews" CASCADE;
            """)
            
            history.to_sql('fctHistory',conn,if_exists='replace',index=False,schema='ProductAnalysis')
            analysis.to_sql('aggReviewAnalysis',conn,if_exists='replace',index=False,schema='ProductAnalysis')
            reddit.to_sql('stgRedditPosts',conn,if_exists='replace',index=False,schema='ProductAnalysis')
            reviews.to_sql('stgBestbuyReviews',conn,if_exists='replace',index=False,schema='ProductAnalysis')
            
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
    
            
        
        
        
        

        

        
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from Backend.handlers.credentials_handler import getCredentials
import os 

url = getCredentials()[-1]


class Migrations:
    def __init__(self,url):
        assert os.path.exists('MigrationScripts/database_scripts')
        assert os.path.exists('MigrationScripts/local_scripts')
        
        self.engine = create_engine(url)
        
        self.local_scripts = self.getLocalScripts()
        self.database_scripts = self.getDatabaseScripts()
        
    def getLocalScripts(self):
        scripts = {}    
        for file in os.listdir('MigrationScripts/local_scripts'):
            key = file.split('_')[0]
            if key not in scripts.keys():
                scripts[key] = []
            with open('MigrationScripts/local_scripts/'+file,'r') as f:
                scripts[key].append(f.read())
        return scripts
    
    def getDatabaseScripts(self):
        scripts = {}   
        for file in os.listdir('MigrationScripts/database_scripts'):
            key = file.split('_')[0]
            if key not in scripts.keys():
                scripts[key] = []
            with open('MigrationScripts/database_scripts/'+file,'r') as f:
                scripts[key].append(f.read())
        return scripts

    def runCheck(self):
        found = 1
        with self.engine.connect() as conn:
            for script in self.local_scripts['V0']:
                try:
                    found *= pd.read_sql(script,conn)['count'].values.tolist()[0]
                except:
                    found = 0
                    break
        return found    
    
    def runDatabase(self):
        if self.runCheck() == 1:
            frames = []
            with self.engine.connect() as conn:
                for script in self.database_scripts['V2'] + self.database_scripts['V3']:
                    frames.append(pd.read_sql(script,conn))
                for frame in frames:
                    name = 'agg'+''.join([name.capitalize() for name in frame.columns.tolist()[3].split('_')])
                    frame.to_sql(name,conn,if_exists='replace',index=False,schema='ProductAnalysis')
    
    def runLocal(self):
        if self.runCheck() == 1:
            frames = []
            with self.engine.connect() as conn:
                unique_entries = pd.read_sql(self.local_scripts['V1'][0],conn)['PK_Md'].tolist()
                for entry in unique_entries:
                    for script in self.local_scripts['V2'] + self.local_scripts['V3']:
                        frames.append(pd.read_sql(script.replace('PLACEHOLDER',entry),conn))
            for frame in frames:
                self.plotdf(frame)
    
    def plotdf(self,df):
        plt.figure(figsize=(24, 12))
        columns = df.columns.tolist()
        df['reviewDate'] = pd.to_datetime(df['reviewDate'])
        plt.plot(df[columns[1]],df[columns[2]],marker = 'o', linestyle='-')

        plt.xlabel('Date')
        plt.ylabel(columns[2])
        plt.title(df['product'].values.tolist()[0])
        product_name = df["product"].values.tolist()[0].replace("'","").replace('"','')

        if not os.path.isfile(f'./MigrationScripts/figures/{product_name}'):
            os.makedirs(f"{os.getcwd()}\\MigrationScripts\\figures\\{product_name}",exist_ok=True)

        plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d'))

        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'./MigrationScripts/figures/{product_name}/{columns[2]}.png')

        plt.close()
        
    def runAll(self):
        try: 
            self.runDatabase()
            self.runLocal()
            return True
        except:
            return False
                    


        
    
    
        
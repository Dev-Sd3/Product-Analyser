import sqlalchemy
import pandas as pd


def getItems(list):
    list = list.split(',')
    list = [word.strip() for word in list]
    return list


def getDatabase():
    with open('Backend/Credentials/url.txt') as f:
        url = f.read()
    engine = sqlalchemy.create_engine(url)

    with engine.connect() as con:
        try:
            Historydf = pd.read_sql(
                'SELECT * FROM "ProductAnalysis"."fctHistory"', con)
            Analysisdf = pd.read_sql(
                'SELECT * FROM "ProductAnalysis"."aggReviewAnalysis"', con)  
            historyEntries = Historydf['Product'].values.tolist()
            formatted_entries = []
            for entry in historyEntries:
                entry = entry.split(' ')
                entry = entry[0:int(len(entry)*0.9)]
                formatted_entries.append(' '.join(entry))

            return Historydf, Analysisdf, formatted_entries, historyEntries
        except:
            return pd.DataFrame(), pd.DataFrame(), [], []


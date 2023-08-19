from pytrends.request import TrendReq
import pandas as pd
import matplotlib.pyplot as plt
from sentence_transformers import SentenceTransformer, util
from Backend.handlers.logging_handler import logger
model = SentenceTransformer('paraphrase-MiniLM-L6-v2')


def find_closest_match(sentence, sentence_list):
    sentence_embedding = model.encode(sentence, convert_to_tensor=True)
    list_embeddings = model.encode(sentence_list, convert_to_tensor=True)
    similarity_scores = util.pytorch_cos_sim(
        sentence_embedding, list_embeddings)
    closest_idx = similarity_scores.argmax().item()
    closest_match = sentence_list[closest_idx]
    return closest_match


def find_closest_matches_list(list_A, list_B):
    closest_matches = []
    for sentence in list_A:
        try:
            closest_match = find_closest_match(sentence, list_B)
            closest_matches.append(closest_match)
        except:
            closest_matches.append('NOTFOUND')
    return closest_matches



class GoogleTrends:
    def __init__(self,products):
        
        self.pytrends = TrendReq(hl='en-US', 
                    tz=180, 
                    retries=2, 
                    backoff_factor=0.1, 
                    requests_args={'verify':True}
                   )
        
        #self.products = self.getClosestMatches(products)   
        self.products = products
        
    def getClosestMatches(self,products):
        closest_matches = []
        for product in products:
            suggestions = [suggestions["title"] for suggestions in self.pytrends.suggestions(product)]
            closest_matches.append(find_closest_matches_list(products,suggestions)[0])
        return closest_matches
        
        
    def getDataframe(self,product,pk):
        query = [product]
        self.pytrends.build_payload(query, timeframe='today 12-m', geo='', gprop='')
        df = self.pytrends.interest_over_time()
        
        date = df.index
        relevancy = df[df.columns.tolist()[0]]
        pks = [pk] * len(df) 
        product = [df.columns.tolist()[0]]*len(df)
        data = list(zip(date,product,pks,relevancy))
        
        return pd.DataFrame(data,columns=['date','product','PK_Md','relevancy'])
    
    def writeFrame(self,pks):
        try:
            frames = [self.getDataframe(product,pk) for product,pk in zip(self.products,pks)]
            frames = [frame for frame in frames if len(frame) > 10]

            stgGoogleTrends = pd.concat(frames).drop_duplicates()
            stgGoogleTrends.to_csv('Backend/Data/stgGoogleTrends.csv')
            return stgGoogleTrends
        except:
            logger.error("Google Trends: Error while acquiring dataframe")
            return False
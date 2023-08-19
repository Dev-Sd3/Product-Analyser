import praw
import pandas as pd
import hashlib
import praw
from praw.models import MoreComments
import pandas as pd
import os
from sentence_transformers import SentenceTransformer, util
from Backend.handlers.logging_handler import logger
from threading import Thread
model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

'''
Reddit handler

After alot of experimenting and tests, the method implemented to find reddit sentiment is to search for posts
reviewing a certain product, then scraping the comment section since people on reddit seem to love giving their opinion
in response to other people's review

previous attemps include:

searching a product's respective subreddit (not always doable)
searching for post mentioning the product (gets junk data by people advertising / selling their product)


all data is then fused and exported to postGres by the Database handler

if not fullRefresh and os.path.exists(f'./Backend/RedditData/{formatted_product}.csv'):
    old_df = pd.read_csv(
        f'./Backend/RedditData/{formatted_product}.csv')
    df = pd.concat(
        [old_df, df], ignore_index=True).drop_duplicates()
df.to_csv(
    f"Backend/RedditData/{formatted_product}.csv", index=False)
return df

'''

class ThreadWithReturnValue(Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args,
                                        **self._kwargs)

    def join(self, *args):
        Thread.join(self, *args)
        return self._return


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



class RedditHandler:
    def __init__(self, cid, sec, usr, psw, agt, products, limit):
        try:
            # assert os.path.isfile('RedditData/subreddits.csv')
            # self.closestSubs = self.getClosestSubs(data)

            self.scraper = praw.Reddit(client_id=cid,
                                       client_secret=sec,
                                       user_agent=agt,
                                       username=usr,
                                       password=psw)
            self.products = products
            self.limit = limit

            history_titles = find_closest_matches_list(
                products, pd.read_csv('Backend/Data/History.csv')['Product'].tolist())
            self.matches = dict(zip(products, history_titles))

            logger.info("Connected to Reddit")
        except:
            logger.error("couldnt connect to reddit in given environement")

    def findData(self, query, match, fullRefresh=True):
        if match == 'NOTFOUND':
            return
        formatted_product = match.replace(' ', '-').replace(',', '').replace('.','').replace('"','')
        sub = self.scraper.subreddit('all')
        posts = [post for post in sub.search(query=f'{query} review', sort='gilded', syntax='plain', limit=self.limit)
                    if 'opinion' in post.title.lower()
                    or 'review' in post.title.lower()]
        df = self.getData(posts, query, match)
        
        if fullRefresh:
            df.to_csv(f'Backend/RedditData/{query}.csv', index=False)
            return df
        else:
            if os.path.exists(f'Backend/RedditData/{formatted_product}.csv'):
                old_frame = pd.read_csv(f"Backend/RedditData/{formatted_product}.csv")
                merged = pd.concat([old_frame, df], ignore_index=True).drop_duplicates(subset=['reviewId'])
                merged.to_csv(f"Backend/RedditData/{formatted_product}.csv", index=False)
                return merged
            else:
                df.to_csv(f'Backend/RedditData/{formatted_product}.csv', index=False)
                return df
            
    def getData(self, posts, query, match):
        formatted_product = query.replace(' ', '-').replace(',', '')
        all_posts = []
        for post in posts:
            for comment in post.comments:
                if isinstance(comment, MoreComments):
                    continue
                PK_md = hashlib.md5(match.encode()).hexdigest()
                reviewId = comment.id
                subreddit = str(post.subreddit)
                postTitle = post.title
                pid = post.id
                text = comment.body
                score = comment.score
                author = str(comment.author)
                entry = [PK_md, reviewId, match, query,
                         subreddit, postTitle, pid, text, score, author]
                all_posts.append(entry)
        cols = ['PK_Md', 'reviewId', 'product', 'query', 'subreddit',
                'postTitle', 'postId', 'comment', 'score', 'author']
        df = pd.DataFrame(all_posts, columns=cols)
        return df

    def MultiThreadedAcquireData(self, fullRefresh=True):
        Threads = []
        frames = []
        for product, title in self.matches.items():
            thr = ThreadWithReturnValue(
                target=self.findData, args=(product, title, fullRefresh,))
            thr.start()
            Threads.append(thr)
        for t in Threads:
            frames.append(t.join())

        logger.info("Acquired data from Reddit")
        return pd.concat(frames, ignore_index=True)

#     def getClosestSubs(self,data):
#         closest_subs = []
#         subs = pd.read_csv('RedditData/subreddits.csv')['name'].tolist()
#         for product in data:
#             product = ' '.join(product.split(' ')[0:4])
#             if difflib.get_close_matches(product,subs):
#                 closest = difflib.get_close_matches(product,subs)[0]
#             else:
#                 closest = 'NotFound'
#             closest_subs.append(closest)
#         return closest_subs

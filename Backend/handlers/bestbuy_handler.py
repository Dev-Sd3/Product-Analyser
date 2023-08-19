from bs4 import BeautifulSoup
from fake_headers import Headers
from Backend.handlers.logging_handler import logger
from selenium import webdriver
import time
import pandas as pd
import hashlib
import os
import threading
from threading import Thread

sem = threading.Semaphore()

class ThreadWithReturnValue(Thread):
    
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        try:
            if self._target is not None:
                self._return = self._target(*self._args,
                                                    **self._kwargs)
        except Exception as e:
            logger.error(f'Thread {self._args[0]} failed!')
            logger.error(e)
            raise e
            self._return = False
            
    def join(self, *args):
        Thread.join(self, *args)
        return self._return


class BestBuy:
    def __init__(self,entry):
        self.urlMode = False
        if ':' in entry and entry.split(':')[0] == 'https':
            self.urlMode = True
        
        self.initialiseConnection()
        
        if self.urlMode:
            logger.info('url provided')
            self.closest_url = entry
            self.oldDataframe, self.title = self.getOldDataframe(entry)
        else:
            logger.info('product provided')
            self.product = entry[0:45]
            self.closest_url, self.title = self.getUrlTitle()
            history = pd.read_csv('Backend/Data/History.csv')
            if self.closest_url in history['url'].values.tolist():
                raise Exception('Product already exists in database')
            
    
    def FindReview(self, tag):
        return tag.name == 'li' and tag.has_attr('class') and tag['class'] == ['review-item']

    def FindRecommendation(self, tag):
        return tag.name == 'div' and tag.has_attr('class') and tag['class'] == ['ugc-recommendation']

    def FindProduct(self, tag):
        return tag.name == 'h4' and tag.find('a') and tag.find('a').has_attr('href')
            
            
    def getUrlTitle(self):
        closest_url, title = self.getReviewsLink()
        if closest_url:
            return closest_url, title
        else:
            logger.error('Failed to find product title')
            self.driver.quit()
            return False, False
        
    def getReviewsLink(self):
        ProductLink = self.getSearchLink()
        if ProductLink:
            try:
                self.driver.get(ProductLink)
            except:
                logger.fatal(
                    f"Couldn't connect to {ProductLink} due to connection problem, terminating search")

            soup = BeautifulSoup(
                self.driver.page_source.encode("utf-8"), 'html.parser')

            if soup.find(self.FindProduct):
                link = 'https://www.bestbuy.com' + \
                    soup.find('h4').find('a')['href']
                link_arr = link.split("/")
                link_arr.insert(4, 'reviews')
                link_arr[-1] = str(link_arr[-1])+("&sort=MOST_RECENT&page=1")
                link_arr[-1] = str(link_arr[-1]).replace('.p?', '?variant=A&')
                link = '/'.join(link_arr)
                logger.info(f'Found product review link {link}')
                self.driver.get(link)
                title = self.driver.title.replace('Customer Reviews: ', '').replace(
                    ' - Best Buy', '').replace('/', '').replace("\\", '')
                return link, title
        logger.error('Couldnt find product link!')
        return False, False
    
    def getSearchLink(self):
        url = f"https://www.bestbuy.com/site/searchpage.jsp?st={self.product}&_dyncharset=UTF-8&_dynSessConf=&id=pcat17071&type=page&sc=Global&cp=1&nrp=&sp=&qp=&list=n&af=true&iht=y&usc=All+Categories&ks=960&keys=keys"
        logger.info(f'searching url {url}')

        try:
            self.driver.get(url)
        except:
            logger.fatal(f"Couldn't connect to {url}, terminating search")

        soup = BeautifulSoup(
            self.driver.page_source.encode("utf-8"), 'html.parser')
        for link in soup.find_all('a'):
            if 'United states'.lower() in link.text.lower():
                logger.info(f'Successfully redirected to US store')
                return link['href']
        logger.error('couldnt find redirect link to us store')
        return False
            
    def getOldDataframe(self,key):
        history = pd.read_csv('Backend/Data/History.csv')
        if ':' in key and key.split(':')[0] == 'https':
            name = history[history['url'] == key]['Version'].values[0]
            title = history[history['url'] == key]['Product'].values[0]
            df = pd.read_csv(f'Backend/Data/{name}') 
            return df,title       
    
    def initialiseConnection(self):
        header = Headers(
            browser="chrome",
            os="win",
            headers=True)
        FakeHeader = header.generate()
        try:
            self.driver = webdriver.Chrome('Backend/Driver/chromedriver.exe')
            self.driver.minimize_window()
            logger.info('Connection initialized')
        except:
            logger.error("Couldn't connect to driver")
            raise Exception("Failed to connect")
        
    def getDataframe(self, oldDataframe=[], update=False):
        url = self.closest_url
        if url:
            if len(oldDataframe) == 0 or not update:
                logger.info('initialised fresh search')
                All_Reviews = []
            else:
                logger.info('initialised search in update mode')
                All_Reviews = oldDataframe.values.tolist()
            page = 1
            Continue = True
            strikes = 0
            reviews_found = 0
            while Continue:
                url = url.split('=')
                url[-1] = str(page)
                url = '='.join(url)

                try:
                    self.driver.get(url)
                except:
                    logger.fatal(
                        f"Couldn't connect to {url}, terminating search")
                
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
                if "Page Not Found" in soup.title.text:
                    logger.error('Page not found, terminating search')
                    self.abortNewEntry()
                    break
                
                for element in soup.find_all(self.FindReview):
                    review = self.getReviews(url, element)
                    if review[1] not in [rid[1] for rid in All_Reviews]:
                        if update:
                            All_Reviews.insert(0, review)
                        else:
                            reviews_found += 1
                            All_Reviews.append(review)
                        strikes = 0
                    else:
                        strikes += 1
                        if strikes == 3:
                            Continue = False
                            break
                page += 1

            logger.info(f'Quit search after adding {reviews_found} reviews')
            self.driver.quit()
            
            df = pd.DataFrame(All_Reviews, columns=['PK_Md', 'reviewId', 'product', 'reviewDate',
                        'reviewerName', 'review', 'rating', 'recommendation', 'url'])
            
            return df
        else:
            logger.error('Couldnt find product!')
            return False        
            
    def getReviews(self, url, element):
        rating_review = []
        for txt in element.find_all('p'):
            rating_review.append(txt.text)
        product = self.title
        PK_md = hashlib.md5(self.title.encode()).hexdigest()
        user_id = self.getId(element)
        name = element.find('strong').text
        unformatted_date = element.find('time')
        date_arr = unformatted_date['title'].split(' ')
        formatted_date = date_arr[1].replace(
            ",", "") + " " + date_arr[0] + " " + date_arr[2]
        formatted_date = pd.to_datetime(formatted_date, format='%d %b %Y')
        formatted_date = str(formatted_date.date())
        review = rating_review[1].replace("'", '').replace('"', '').replace('\\n', '\n').replace(
            "’", "").replace(',', '').replace("’", "").replace(',', '').replace('\n', '')
        if element.find(self.FindRecommendation):
            recommendation = 'no' not in element.find(
                self.FindRecommendation).text.lower()
        elif int(rating_review[0].split(' ')[1]) > 3:
            recommendation = True
        else:
            recommendation = False
        return [PK_md, user_id, product, formatted_date, name, review, int(rating_review[0].split(' ')[1]), recommendation, url]
    
    def getId(self, soup):
        if soup:
            user_id = soup.find('h4')['id']
            user_id = user_id.split('-')
            user_id.pop(0)
            user_id.pop(0)
            user_id.pop(0)
            return ''.join(user_id)
        return None

    def acquireData(self):
        
        if self.urlMode:
            logger.info(f'Acquisition of url |{self.closest_url}| initiated')
            df = self.getDataframe(oldDataframe=self.oldDataframe, update=True)
            self.updateHistory(df)
            return True, df
            
        elif self.product != "" and type(self.title) != bool and 'sign in' not in self.title.lower():
            logger.info(f'Acquisition of product |{self.product}| initiated')
            df = self.getDataframe()
            self.appendHistory(df)
            return True, df
            
    def appendHistory(self,df):
        Time = time.ctime().replace('  ', ' ').replace(' ', '-').replace(':', '-')
        illegal_characters = '/ \ : * ? " < > |'.split(' ')
        formatted_product = ''.join(
            [char for char in self.title if char not in illegal_characters]).replace('  ', ' ')
        Version = f"BestBuyAll-{formatted_product}-{Time}.csv"
        df.to_csv(f"Backend/Data/{Version}", index=False)
        history = pd.read_csv('Backend/Data/History.csv')
        entry = pd.DataFrame({
            'Product': self.title,
            'Version': Version,
            'PK_Md': hashlib.md5(self.title.encode()).hexdigest(),
            'url': self.closest_url,
            'date': Time
        }, index=[0])
        newHistory = pd.concat([entry, history], ignore_index=True)
        newHistory.to_csv('Backend/Data/History.csv', index=False)
        return True
    
    def updateHistory(self,df):
        Time = time.ctime().replace('  ', ' ').replace(' ', '-').replace(':', '-')
        illegal_characters = '/ \ : * ? " < > |'.split(' ')
        formatted_product = ''.join(
            [char for char in self.title if char not in illegal_characters]).replace('  ', ' ')
        newVersion = f"BestBuyAll-{formatted_product}-{Time}.csv"
        df.to_csv(f"Backend/Data/{newVersion}", index=False)
        history = pd.read_csv('Backend/Data/History.csv')
        history.at[history.loc[history['Product'] == self.title]['Version'].index.values[0],'Version'] = newVersion
        history.at[history.loc[history['Product'] == self.title]['date'].index.values[0],'date'] = Time
        history.to_csv('Backend/Data/History.csv', index=False)
        return True
        
def ThreadFunction(product):
    Bb = BestBuy(product)
    flag, result = Bb.acquireData()
    if flag:
        return result

def MultiThreadedAcquisition(products,fullRefresh,append=False):
    
    if fullRefresh or append:
        arguments = products
    else:
        history = pd.read_csv('Backend/Data/History.csv')
        arguments = history['url'].values.tolist()
    
    Dataframes = []
    Threads = []
    for p in arguments:
        thr = ThreadWithReturnValue(target=ThreadFunction,args = (p,))
        thr.start()
        Threads.append(thr)

    for index,t in enumerate(Threads):
        df = t.join()
        if type(df) != bool:
            Dataframes.append(df)
        
    stgBestbuyReviews = pd.concat(Dataframes)
    
    return stgBestbuyReviews
            
        

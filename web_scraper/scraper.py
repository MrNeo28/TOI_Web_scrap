import requests
import time 

import pandas as pd

from pathlib import Path
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta

from typing import List, Tuple, Optional, Any
from utils.logger import print_log

class toi_scrap():
    def __init__(self, max_entries:int, min_entries:int, max_sleep:int, init_date: Tuple[int]=(2022, 8, 1), iter_date: Tuple[int]=(2022, 8, 1)):
        self.init_date = init_date
        self.iter_date = iter_date
        self.max_entries = max_entries
        self.min_entries = min_entries
        self.max_sleep = max_sleep
        self.url_correct = "http://timesofindia.indiatimes.com/"
        self.archive_url = "https://timesofindia.indiatimes.com/2019/2/7/archivelist/year-2019,month-2,starttime-43503.cms"
        self.url_standard = 'http://'
        self.url_inside = '.indiatimes.com/'
        self.res = {"date":[], "headline":[], "link":[]}

    
    def create_pd(self, dict_df):
        """
        Create dataframe with columns -> [id, date, author, headline, link, category]
        """
        cols = ["date", "headline", "link",]
        df = pd.DataFrame(dict_df, columns=cols)
        return df.to_csv("news.csv")

    def get_last_valid_date(self):
        """
        Indian Standard Time (IST) is 5.5 hours ahead of Coordinated Universal Time (UTC), 
        we can shift the UTC time to 5hrs and 30 mins.
        """
        return datetime.utcnow() + timedelta(hours=5, minutes=30)
    
    def get_next_day(self, year: int, month: int, day: int) -> Tuple[int]:
        next_day = datetime(year, month, day) + timedelta(days = 1)
        return (next_day.year, next_day.month, next_day.day)

    def is_valid_date(self, year:int, month:int, day:int) -> bool:
        try:
            date(year, month, day)
        except ValueError:
            return False
        
        current = datetime(year, month, day)
        ist = self.get_last_valid_date()
        return current + timedelta(days=1) < ist and current > datetime(*self.init_date) 
        
    def is_valid_url(self, url: str) -> str | None:
        if not url.startswith(self.url_standard) or not self.url_inside in url:
            if not url.endswith('.cms') or 'http' in url or ' ' in url:
                return None
            else:
                return self.url_correct + url
        
        return url
    
    def get_url_for_day(self, year:int, month:int, day:int) -> str | None:
        if not self.is_valid_date(year, month, day):
            return None
        
        day_count = (date(year, month, day) - date(1900, 1, 1)).days + 2
        return "http://timesofindia.indiatimes.com/{year}/{month}/{day}/archivelist/year-{year},month-{month},starttime-{daycount}.cms".format(
            year = year,
            month = month,
            day = day,
            daycount = day_count
        )
    
    def get_content(self, url:str, datetuple:Tuple[int]= (2002, 8, 9)) -> List[str]:
        print_log(f"Request sent to url {url}")
        req = requests.get(url)
        print_log("Url retrive, now prasing")
        soup = BeautifulSoup(req.content, "html.parser")

        divs = soup.find_all('div', style='font-family:arial ;font-size:12;font-weight:bold; color: #006699')
        if not len(divs) == 1:
            error_str = "Found {0} divs matching signature. Aborting.".format(len(divs))
            self.error(error_str)
            raise Exception(error_str)
        articles = divs[0].find_all('a')
        print_log(f"Found {len(articles)} hyperlinks in the archive.")
        articles = [a for a in articles if len(a.text) > 0]
        res = []
        titles = set({})
        for art in articles:
            corr_url = self.is_valid_url(art['href'])
            if corr_url:
                if art.text in titles:
                    continue
            titles.add(art.text)
            res.append([
                datetime(*datetuple).strftime('%Y-%m-%d'),
                art.text,
                corr_url,
                ])
        print_log(f"Finished parsing, {len(res)} rows remain")
        return res
    
    def get_article_for_day(self, year:int, month:int, day:int):
        print_log("Getting article for the day")
        url = self.get_url_for_day(year, month, day)
        if not url:
            return 0
        data = self.get_content(url, (year, month, day))
        return data
    
    def __call__(self):
            res = {'date': [], 'headline': [], 'link': []}
            while self.init_date != self.iter_date and self.is_valid_date(*self.init_date):
                next_date = datetime(*self.init_date) + timedelta(days=1)
                sec_to_next_date = (next_date - self.get_last_valid_date()).seconds
                print_log("Reached the end, {0} seconds until {1}".format(sec_to_next_date, datetime(*self.iter_date).strftime('%Y-%m-%d')))
                if sec_to_next_date <= self.max_sleep:
                    time.sleep(sec_to_next_date)
                else:
                    print_log('Seconds till next day {0} greater than {1}, so only sleeping for {1}'.format(sec_to_next_date, self.max_sleep))
                    time.sleep(self.max_sleep)

                print_log('Woken up, getting init date again')
                self.init_date = self.get_next_day(*self.init_date)
                print_log('New date set to {0}'.format(self.init_date))
            print_log("Retrieving articles for date {0}".format(self.init_date))
            data = self.get_article_for_day(*self.init_date)
            try:
                num_rows = len(data)
            except TypeError:
                num_rows = 0
            print_log("Retrieved {0} rows from TOI".format(num_rows))
            if num_rows == 0:
                print_log("Sleeping for 10 seconds, no rows retrieved")
                time.sleep(10)
            else:
                self.init_date = self.get_next_day(*self.init_date)
                print_log("Iterated to next day - {0}".format(datetime(*self.init_date)))
                res['date'].append[data[0]]
                res['headline'].append[data[1]]
                res['link'].append[data[2]]
            return res

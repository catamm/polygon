# import functions
from adapter import data_reader

# import variables
from variables import alphavantage_url, alphavantage_api_key

# import module
import pandas as pd
from datetime import date, datetime
import time

def alfa(day):
    feed_dict = {'title': [], 'url': [], 'time_published': [], 'authors': [], 'summary': [], 'banner_image': [],
                'source': [], 'category_within_source': [], 'source_domain': [], 'overall_sentiment_score': [],
                'overall_sentiment_label': [], 'topics': [], 'ticker_sentiment': []}

    day_news=pd.DataFrame(feed_dict)
    for hour in range(0,24,2):
        hours = f"0{hour}" if hour < 10 else f"{hour}"
        reply = data_reader(f'{alphavantage_url}{day.strftime("%Y%m%dT")}{hours}00{alphavantage_api_key}','feed')
        if reply is None:
            pass
        else:
            day_news = pd.concat([day_news, reply], ignore_index=True, axis=0)
            day_news = day_news.astype(str).drop_duplicates()
            time.sleep(13)


"""
Created on Thu Jun  4 13:28:22 2020

@author: dyoo
@updated by mtra
"""

import requests
import numpy as np
import pandas as pd
import time
import random

# NEWSAPI

# set up variables
city = 'texas'
start_dt = '2020-06-03'
end_dt = '2020-06-03'
sort_by = 'popularity'
page_size = '20'
api_key = '1b8c3d6854b74d3a89f59ad40b8ebbef'

params_page = dict(q=city, _from=start_dt, to=end_dt,
                   sortBy=sort_by, pageSize=page_size, apiKey=api_key)

newsApi_url = 'https://newsapi.org/v2/everything?'


def get_news_articles(url_base, params):

    df_out = pd.DataFrame()

    try:
        response = requests.get(url_base, params=params)
        response.raise_for_status()

        num_pages = int(np.ceil(response.json()['totalResults']/100))
        n_pag = min(num_pages, 500)

        for page in range(1, num_pages+1):
            res = requests.get(url_base, params=params)

            if response.status_code == 200:
                print(f'Processing page {page}')
                json_res = res.json()
                article_dataFrame = pd.DataFrame(json_res['articles'])

                # get source name and assign source api to newsapi
                article_dataFrame['source_name'] = article_dataFrame['source'].apply(
                    lambda x: x['name'])
                article_dataFrame['source_api'] = 'newsapi'

                # only take relevant columns
                article_dataFrame = article_dataFrame[[
                    'source_name', 'source_api', 'title', 'publishedAt']]
                df_out = pd.concat([df_out, article_dataFrame])

            else:
                print('Error:', response.status_code)

            time.sleep(random.random() * random.randint(1, 2))

    except requests.exceptions.HTTPError as err:
        print(err)

    return df_out


newApi_df = get_news_articles(url_base=newsApi_url, params=params_page)

# NYT

# set up variables
nyt_url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'

params_nyt = {
    'q': 'election',
    'api-key': 'oPEMM8iYo0Vj5AjUxzMEB88IYj5tqrRO',
    # 'begin_date' : int(start_dt.replace('-', '')),
    # 'end_date' : int(end_dt.replace('-', '')),
    'fq': 'news_desk:("Business") AND glocations:("TEXAS")',
    'page': 0}


def get_nyt_articles(url_base, params):
    df_out = pd.DataFrame()

    for page in range(0, 100):
        params_pg = params
        params_pg['page'] = page

        try:
            response = requests.get(url_base, params=params)
            response.raise_for_status()

            if response.status_code == 200:
                print(f'Processing page {page}')
                json_res = response.json()
                article_dataFrame = pd.DataFrame(json_res['response']['docs'])

                if not article_dataFrame.empty:

                    # assign source api to nyt
                    article_dataFrame['source_api'] = 'nyt'

                    # only take relevant columns
                    article_dataFrame = article_dataFrame[[
                        'source', 'source_api', 'abstract', 'pub_date']]
                    df_out = pd.concat([df_out, article_dataFrame])

            else:
                print('Error:', response.status_code)

            time.sleep(random.random() * random.randint(1, 2))

        except requests.exceptions.HTTPError as err:
            print(err)
            break

    return(df_out)


nyt_df = get_nyt_articles(url_base=nyt_url, params=params_nyt)

# rename columns
nyt_df.rename(columns={'source': 'source_name', 'source_api': 'source_api',
                       'abstract': 'title', 'pub_date': 'publishedAt'}, inplace=True)

# concat data
df_final = pd.concat([newApi_df, nyt_df])


# NYT Archives

# setup variables
params_archives_nyt = {
    'api-key': 'oPEMM8iYo0Vj5AjUxzMEB88IYj5tqrRO'
}


def get_nyt_archives(params, year, start_mth, end_mth, subject, location=""):

    df_out = pd.DataFrame()

    for month in range(start_mth, end_mth+1):
        url_base = f'https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?'

        try:
            response = requests.get(url_base, params=params)
            response.raise_for_status()

            if response.status_code == 200:
                json_res = response.json()

                out_list = []
                for article in json_res['response']['docs']:
                    for keyword in article['keywords']:
                        if (keyword['name'] == 'subject' and keyword['value'] == subject):  # \
                            # and (keyword['name'] == 'glocations' and keyword['value'] == location):
                            out_list.append(article)

                article_dataFrame = pd.DataFrame(out_list)

                if not article_dataFrame.empty:

                    # assign source api to nyt
                    article_dataFrame['source_api'] = 'nyt_archives'

                    # only take relevant columns
                    article_dataFrame = article_dataFrame[[
                        'source', 'source_api', 'abstract', 'pub_date']]
                    df_out = pd.concat([df_out, article_dataFrame])

            else:
                print('Error:', response.status_code)

        except requests.exceptions.HTTPError as err:
            print(err)
            break

        time.sleep(random.random() * random.randint(1, 2))

        df_out.drop_duplicates(inplace=True)
        df_out.reset_index(inplace=True, drop=True)

    return(df_out)


oil_archives = get_nyt_archives(params=params_archives_nyt, year=2020,
                                start_mth=1, end_mth=3, subject="Oil (Petroleum) and Gasoline")  # location="Texas"

retail_archives = get_nyt_archives(
    params=params_archives_nyt, year=2020, start_mth=1, end_mth=3, subject="Shopping and Retail")  # location="New York City"


# GUARDIAN

params_guardian = {
    'api-key': '61f837c5-ab25-4cc6-887d-654b95d93d34'
}


def get_guardian(params, subject='oil'):

    df_out = pd.DataFrame()

    if subject == 'oil':
        url_base = 'http://content.guardianapis.com/business/oilandgascompanies?'
    elif subject == 'retail':
        url_base = 'http://content.guardianapis.com/business/retails?'

    try:
        response = requests.get(url_base, params=params)
        response.raise_for_status()

        num_pages = response.json()['response']['pages']
        n_pag = min(num_pages, 500)

        for page in range(1, num_pages+1):
            res = requests.get(url_base, params=params)

            if response.status_code == 200:
                print(f'Processing page {page}')
                json_res = res.json()
                article_dataFrame = pd.DataFrame(json_res['articles'])

                # get source name and assign source api to newsapi
                article_dataFrame['source_name'] = article_dataFrame['source'].apply(
                    lambda x: x['name'])
                article_dataFrame['source_api'] = 'newsapi'

                # only take relevant columns
                article_dataFrame = article_dataFrame[[
                    'source_name', 'source_api', 'title', 'publishedAt']]
                df_out = pd.concat([df_out, article_dataFrame])

            else:
                print('Error:', response.status_code)

            time.sleep(random.random() * random.randint(1, 2))

    except requests.exceptions.HTTPError as err:
        print(err)

    return(df_out)

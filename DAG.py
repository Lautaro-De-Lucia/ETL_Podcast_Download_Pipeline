# Import Libraries for Airflow
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime,date,timedelta
# Import Libraries for Python
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import yaml
import sqlite3
import wget
# Python utility functions
from datetime import date,timedelta
from dateutil.relativedelta import relativedelta
from datetime import datetime
import re
import pendulum

def das_printer(to_print):
    print(to_print)

def string_to_delta(s):
    value = int(re.search(r'\d+', s).group())
    if "day" in s:
        value = value*1
    elif "month" in s:
        value = value *30
    date_ago = (datetime.now() - timedelta(days=value)).date()
    str_date= date_ago.strftime("%b %d, %Y")
    return str_date

def get_time_range(n_days):
    days = []
    today = date.today()
    for i in range(0,n_days):
        past = today - timedelta(days=i)
        days.append(past.strftime("%b %d, %Y"))
        days.append(str(i)+' days ago')
    return days

def to_datetime(s):
    date = datetime.strptime(s,"%b %d, %Y")
    return date.strftime("%Y-%m-%d")

@dag(
    dag_id='google_podcast_etl',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 1),
    catchup=False,
)
def google_podcast_etl():

    @task()
    def collect_urls():
        URLs = []
        with open('podcast_urls.txt') as input_file:
            lines = input_file.readlines()
            for line in lines:
                URLs.append(line.strip())
        das_printer(URLs)
        return URLs

    @task()
    def collect_configuration():
        config = dict()
        with open("config.yaml", "r") as f:
            config = yaml.load(f,Loader=yaml.FullLoader)
        das_printer(config)
        return config

    @task()
    def join_sources(urls:list,config:dict):
        source_data = dict()
        source_data['urls']=urls
        source_data['path'] = config['path']
        source_data['days_prior'] = config['days_prior']
        return source_data

    @task()
    def get_metadata(source_data:dict):
        print("Starting load:")
        podcast_metadata_df = pd.DataFrame(columns=['podcast', 'episode', 'date', 'length'])
        for url in source_data['urls']:
            soup = BeautifulSoup(requests.get(url).text, 'lxml')
            show_title = soup.find('div', {'class': 'ZfMIwb'}).text
            try:
                os.mkdir(source_data['path'] + '/' + show_title.replace(" ", "_"))
            except FileExistsError:
                pass
            for podcast in soup.find_all('a', {'role': 'listitem'}):
                release_date = podcast.find('div', {'class': 'OTz6ee'}).text
                if release_date in get_time_range(source_data['days_prior']):
                    if release_date.endswith('ago'):
                        release_date = string_to_delta(release_date)
                    release_date = to_datetime(release_date)
                    name = podcast.find('div', {'class': 'e3ZUqe'}).text
                    length = podcast.find('span', {'class': 'gUJ0Wc'}).text
                    # I could convert to CSV here then create the additional "load_to_db" task
                    metadata = [show_title, name, release_date, length]
                    podcast_metadata_df.loc[-1] = metadata
                    podcast_metadata_df.index = podcast_metadata_df.index + 1  # shifting index
                    podcast_metadata_df.sort_index(inplace=True)
        metadata_dict = podcast_metadata_df.to_dict()
        return metadata_dict

    @task()
    def load_to_db(metadata_dict:dict):
        df = pd.DataFrame.from_dict(metadata_dict)
        database = "metadata.sqlite"
        connection = sqlite3.connect(database)
        df.to_sql('podcasts_metadata', connection, if_exists='append', index=False)
        connection.close()

    @task()
    def download_podcasts_audio_files(source_data:dict):
        for url in source_data['urls']:
            soup = BeautifulSoup(requests.get(url).text, 'lxml')
            show_title = soup.find('div', {'class': 'ZfMIwb'}).text
            for podcast in soup.find_all('a', {'role': 'listitem'}):
                release_date = podcast.find('div', {'class': 'OTz6ee'}).text
                if release_date in get_time_range(source_data['days_prior']):
                    name = podcast.find('div', {'class': 'e3ZUqe'}).text
                    print("\nDownloading: " + show_title + " - " + name)
                    url = podcast.find('div', {'jsname': 'fvi9Ef'})['jsdata'].split(';')[1]
                    filename = wget.download(url, out=source_data['path'] + '/' + show_title.replace(" ", "_"))
                    os.rename(filename, source_data['path'] + '/' + show_title.replace(" ", "_") + '/' + name + '.mp3')

    @task()
    def end():
        pass

    urls = collect_urls()
    conf = collect_configuration()
    source_data = join_sources(urls,conf)
    metadata_dict = get_metadata(source_data)
    end() << [load_to_db(metadata_dict),download_podcasts_audio_files(source_data)]

summary = google_podcast_etl()
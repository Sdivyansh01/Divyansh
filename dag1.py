from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email_on_failure': True,
   'email_on_retry': False,
   'email': ['your_email@example.com'],
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
}
def fetch_articles(ticker, **kwargs):
   # Dummy function to scrape articles
   url = kwargs['url']
   response = requests.get(url)
   soup = BeautifulSoup(response.content, 'html.parser')
   articles = soup.find_all('article', limit=5)
   data = []
   for article in articles:
       title = article.find('h2').get_text()
       text = article.find('p').get_text()
       data.append({'ticker': ticker, 'title': title, 'text': text})
   return data
def clean_and_process(data):
   df = pd.DataFrame(data)
   df.drop_duplicates(subset=['title'], inplace=True)
   return df
def generate_sentiment_score(text):
   # Mock sentiment API call
   return 0.5
def store_to_db(data):
   conn = sqlite3.connect('/path/to/your/db.sqlite')
   data.to_sql('sentiment_scores', conn, if_exists='append', index=False)
def process_data(ticker, **kwargs):
   data = fetch_articles(ticker, **kwargs)
   clean_data = clean_and_process(data)
   clean_data['sentiment_score'] = clean_data['text'].apply(generate_sentiment_score)
   store_to_db(clean_data)
with DAG(
   'pipeline_1',
   default_args=default_args,
   description='Pipeline 1 for scraping and sentiment analysis',
   schedule_interval='0 19 * * 1-5',
   start_date=datetime(2023, 1, 1),
   catchup=False,
) as dag:
   start = DummyOperator(task_id='start')
   fetch_hdfc = PythonOperator(
       task_id='fetch_hdfc',
       python_callable=process_data,
       op_kwargs={'ticker': 'HDFC', 'url': 'https://yourstory.com/search?q=HDFC'},
   )
   fetch_tata = PythonOperator(
       task_id='fetch_tata',
       python_callable=process_data,
       op_kwargs={'ticker': 'Tata Motors', 'url': 'https://finshots.in/search?q=Tata+Motors'},
   )
   end = DummyOperator(task_id='end')
   start >> [fetch_hdfc, fetch_tata] >> end
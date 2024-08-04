
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
def find_mean_age():
   users = pd.read_csv('http://files.grouplens.org/datasets/movielens/ml-100k/u.user', sep='|', names=['user_id', 'age', 'gender', 'occupation', 'zip_code'])
   mean_ages = users.groupby('occupation')['age'].mean()
   print(mean_ages)
def find_top_movies():
   ratings = pd.read_csv('http://files.grouplens.org/datasets/movielens/ml-100k/u.data', sep='\t', names=['user_id', 'item_id', 'rating', 'timestamp'])
   movies = pd.read_csv('http://files.grouplens.org/datasets/movielens/ml-100k/u.item', sep='|', encoding='latin-1', names=['movie_id', 'title'] + ['dummy']*22, usecols=['movie_id', 'title'])
   movie_ratings = ratings.groupby('item_id')['rating'].count()
   top_movies = movie_ratings[movie_ratings >= 35].sort_values(ascending=False).head(20)
   top_movie_names = movies[movies['movie_id'].isin(top_movies.index)]
   print(top_movie_names)
def find_top_genres():
   
   pass
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email_on_failure': True,
   'email_on_retry': False,
   'email': ['sdivyansh007@gmail.com'],
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
}
with DAG(
   'pipeline_2',
   default_args=default_args,
   description='Pipeline 2 for MovieLens data analysis',
   schedule_interval='0 20 * * 1-5',
   start_date=datetime(2023, 1, 1),
   catchup=False,
) as dag:
   start = DummyOperator(task_id='start')
   mean_age_task = PythonOperator(
       task_id='find_mean_age',
       python_callable=find_mean_age,
   )
   top_movies_task = PythonOperator(
       task_id='find_top_movies',
       python_callable=find_top_movies,
   )
   top_genres_task = PythonOperator(
       task_id='find_top_genres',
       python_callable=find_top_genres,
   )
   end = DummyOperator(task_id='end')
   start >> mean_age_task >> top_movies_task >> top_genres_task >> end

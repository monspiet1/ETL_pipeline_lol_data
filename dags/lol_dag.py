from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys, os
from dotenv import load_dotenv

sys.path.insert(0, '/opt/airflow/src')

from extract_mastery_data import mastery_data
from extract_match_data import match_data
from transform_data import exec_transformations
from load_data import exec_loads

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env' # --> ../config/.env
load_dotenv(env_path)



players_puuids = [
    "5mmIq5J0PT9sFRfB6xGrEqxcVaEQGLBmC7le_tyfF26PcAqFPoQeCmjYgT2OJjDS9B8hdKM367n7aA", # andrey
    "carEkMjfnuzRT_WpE6PqHB4muNKff25c9xMRdCxRyrEm9TagTFDfv7uDBIGa_bHGAMxA-BISDBHRfw",
    "ROMMmVOtXhOrdd1-vEE4fjYFVIwpGZYPeM7sO2SPuOElaTc0nj5U-y6NqR1xXtufs5pibHYz7SPI9w",
    "bSBwx7rukHEZug8N4lAxDggKT27_pgnKNnAu50qq3msJdf2nMizG2VzfHVTmwDsagnwdogrHw66pSQ",
    "5cOVn9ZINUO4Ukd-PVmgtEZHqvu0PikPiUkMTrb6K8CzP5Fd-FWPxNVEWayj6EUQSF_ctmNWmqBVqg",
    "1VvhvasECXxYfpFhkpNH-h-ZYVAn1ZD0YgGM49CW2u6q4o8ISveMwSz7JjRgt8njJptZLshg1WAj5g",
    "fUWcifhm4SBGLeY45I4t2QY0I28Lk62mNtOhER3kZNuh9xeACXZznoBigQJX1DW-yWHTcz2jnXwb_g"
]

url_mastery = f'https://br1.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-puuid/'
url_match_id = f'https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/' 
url_match_data = f'https://americas.api.riotgames.com/lol/match/v5/matches/'

@dag(
    dag_id='lol_pipeline',
    default_args={
        'owner':'airflow',
        'depends_on_post':False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    description='Pipeline ETL - Dados do LoL',
    schedule= '0 */1 * * *',
    start_date=datetime(2026,2,23),
    catchup=False,
    tags=['lol', 'etl', 'dados dos jogadores']
)

def lol_pipeline():

    @task 
    def extract():
        mastery_data(url_mastery, players_puuids)
        match_data(url_match_id, url_match_data, players_puuids)

    @task
    def transform():
        df_matches, df_teams, df_players, df_mastery = exec_transformations()
        # salvar em parquet já que nao se pode acessar os dados de uma task em outra
        df_matches.to_parquet('/opt/airflow/data/temp_matches.parquet', index=False)
        df_teams.to_parquet('/opt/airflow/data/temp_teams.parquet', index=False)
        df_players.to_parquet('/opt/airflow/data/temp_players.parquet', index=False)
        df_mastery.to_parquet('/opt/airflow/data/temp_mastery.parquet', index=False)
    
    @task
    def load():
        import pandas as pd

        df_matches = pd.read_parquet('/opt/airflow/data/temp_matches.parquet')
        df_teams = pd.read_parquet('/opt/airflow/data/temp_teams.parquet')
        df_players = pd.read_parquet('/opt/airflow/data/temp_players.parquet')
        df_mastery = pd.read_parquet('/opt/airflow/data/temp_mastery.parquet')
        exec_loads(df_matches, df_teams, df_players, df_mastery)    

    extract() >> transform() >> load()

lol_pipeline()
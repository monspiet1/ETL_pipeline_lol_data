from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env' # --> ../config/.env
load_dotenv(env_path)

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_NAME')
host = 'postgres'

matches_path = Path(__file__).resolve().parent.parent / 'sql' / 'insert_matches.sql'
teams_path = Path(__file__).resolve().parent.parent / 'sql' / 'insert_teams.sql'
players_path = Path(__file__).resolve().parent.parent / 'sql' / 'insert_players.sql'
mastery_path = Path(__file__).resolve().parent.parent / 'sql' / 'insert_mastery.sql'
def get_engine():
    logging.info(f"-> Conectando em {host}:5432/{database}")
    return create_engine(
        f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:5432/{database}" # --> criando um link pro banco
    )

engine = get_engine()

def load_query(path):
    with open(path, "r") as f:
        return f.read()

query_matches = load_query(matches_path)
query_teams = load_query(teams_path)
query_players = load_query(players_path)
query_mastery = load_query(mastery_path)

def load_matches_data(df: pd.DataFrame):
    # verificar se o ID da partida ja existe na tabela
    data = df.to_dict(orient="records")
    
    with engine.begin() as conn:
        conn.execute(text(query_matches), data)
    
    check = pd.read_sql("SELECT * FROM matches", engine)
    print(f"Tabela matches preenchida com sucesso -> Linhas: {len(check)}")

def load_teams_data(df: pd.DataFrame):
    # verificar se o ID da partida ja existe na tabela
    data = df.to_dict(orient="records")
    
    with engine.begin() as conn:
        conn.execute(text(query_teams), data)
    
    check = pd.read_sql("SELECT * FROM teams", engine)
    print(f"Tabela teams preenchida com sucesso -> Linhas: {len(check)}")

def load_players_data(df: pd.DataFrame):
    # verificar se o ID da partida ja existe na tabela
    data = df.to_dict(orient="records")
    
    with engine.begin() as conn:
        conn.execute(text(query_players), data)
    
    check = pd.read_sql("SELECT * FROM players", engine)
    logging.info(f"Tabela players preenchida com sucesso -> Linhas: {len(check)}")

def load_mastery_data(df: pd.DataFrame):
    # verificar se o ID da partida ja existe na tabela
    data = df.to_dict(orient="records")
    
    with engine.begin() as conn:
        conn.execute(text(query_mastery), data)
    
    check = pd.read_sql("SELECT * FROM mastery", engine)
    logging.info(f"Tabela mastery preenchida com sucesso -> Linhas: {len(check)}")

def exec_loads(df_matches: pd.DataFrame, df_teams: pd.DataFrame, df_players: pd.DataFrame, df_mastery: pd.DataFrame):

    logging.info(f'-> Iniciando o laod dos dados...')

    load_matches_data(df_matches)
    load_teams_data(df_teams)
    load_players_data(df_players)
    load_mastery_data(df_mastery)

    logging.info(f'-> Load executado com sucesso!')


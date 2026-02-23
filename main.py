from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import logging

from src.extract_mastery_data import mastery_data
from src.transform_data import exec_transformations
from src.load_data import exec_loads

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env' # --> ../config/.env
load_dotenv(env_path)

API_KEY = os.getenv('RIOT_API_KEY ')

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

def pipeline():
    try:
        logging.info('ETAPA 1: EXTRACT')
        mastery_data(url_mastery, players_puuids)

        logging.info('ETAPA 2: TRANSFORM')
        df_matches, df_teams, df_players, df_mastery = exec_transformations()

        # Mostra quantas linhas têm match_id nulo
        print('df_players retornado na função main: \n', df_players['match_id'])
        logging.info('ETAPA 3: LOAD')
        exec_loads(df_matches, df_teams, df_players, df_mastery)
    
    except Exception as e:
        logging.error(f"X  ERRO no pipeline: {e}")
        import traceback
        traceback.print_exc()

pipeline()

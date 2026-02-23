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

user = os.getenv('user')
password = os.getenv('password')
database = os.getenv('database')
host = 'localhost'

def get_engine():
    logging.info(f"-> Conectando em {host}:5432/{database}")
    return create_engine(
        f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:5432/{database}" # --> criando um link pro banco
    )

engine = get_engine()

query = """
INSERT INTO matches (match_id, creation_time, duration, end_time)
VALUES (:match_id, :creation_time, :duration, :end_time)
ON CONFLICT (match_id) DO NOTHING
"""

df = pd.DataFrame({
    "match_id": [
        "BR1_3207143905",
        "BR1_3191052198"
    ],
    "creation_time": pd.to_datetime([
        "2026-02-16 23:19:51.054000-03:00",
        "2026-01-13 00:44:50.448000-03:00"
    ]),
    "duration": [
        39.03,
        27.25
    ],
    "end_time": pd.to_datetime([
        "2026-02-17 00:00:03.438000-03:00",
        "2026-01-13 01:13:01.139000-03:00"
    ])
})

data = df.to_dict(orient="records")

with engine.begin() as conn:
    conn.execute(text(query), data)

check = pd.read_sql("SELECT * FROM matches", engine)
print(len(check))

import requests
import json
from pathlib import Path
import logging
import dotenv
import os


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env' # --> ../config/.env
dotenv.load_dotenv(env_path)

API_KEY = os.getenv('API_KEY')

url = f'https://br1.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-puuid/'
headers = { 
    "X-Riot-Token": API_KEY
}

def mastery_data(url: str, puuids: list) -> list:
    mastery_data = []

    for p in puuids:
        complete_url = url + p
        res = requests.get(complete_url, headers=headers)
        mastery_data.append(res.json())

    if res.status_code != 200:
        logging.error(f'Erro na requisição dentro do mastery_data! -> {res.status_code}')
        return []
    
    if not mastery_data:
        logging.warning('Nada retornado')
        return []

    # local de armazenamento dos dados
    output_path = 'data/mastery_data.json'
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:

        json.dump(mastery_data, f, indent=4)

    logging.info(f'Arquivo salvo')
    return mastery_data




    

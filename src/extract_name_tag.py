import requests
import json
from pathlib import Path
import logging
import dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

api_key = 'RGAPI-e80416ef-8317-4239-9709-326acc51a725'
gameName = 'gordin que atiça'
tagName = 'maki'

nicks = [
    'gordin que atiça',
    'nicole assanhada',
    'flor preta',
    'funkeiro com qi',
    'pepsi black',
    'why so serious',
    'Mitski'

]

tags = [ 
    'maki',
    'baka',
    'bill',
    'vasco',
    'sungs',
    '2063',
    'jpb'
]

url = f'https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/'
headers = { 
    "X-Riot-Token": api_key
}

def extract_puuid(url: str, nicks: list, tags: list) -> list:
    data = []

    for nick, tag in zip(nicks,tags):
        complete_url = url + nick + '/' + tag
        res = requests.get(complete_url, headers=headers)
        data.append(res.json())

    if res.status_code != 200:
        logging.error(f'Erro na requisição! -> {res.status_code}')
        return []
    
    if not data:
        logging.warning('Nada retornado')
        return []

    # local de armazenamento dos dados dos players -> nick,tag e puuid
    output_path = 'data/players_data.json'
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:

        json.dump(data, f, indent=4)

    #for d in data:
    logging.info(f'Res retornado -> {data}')
    return data

extract_puuid(url=url, nicks=nicks, tags=tags)
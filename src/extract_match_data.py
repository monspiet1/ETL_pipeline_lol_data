import requests
import json
from pathlib import Path
import logging
import dotenv
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

players_puuids = [
    "5mmIq5J0PT9sFRfB6xGrEqxcVaEQGLBmC7le_tyfF26PcAqFPoQeCmjYgT2OJjDS9B8hdKM367n7aA", 
    "carEkMjfnuzRT_WpE6PqHB4muNKff25c9xMRdCxRyrEm9TagTFDfv7uDBIGa_bHGAMxA-BISDBHRfw", 
    "ROMMmVOtXhOrdd1-vEE4fjYFVIwpGZYPeM7sO2SPuOElaTc0nj5U-y6NqR1xXtufs5pibHYz7SPI9w", 
    "bSBwx7rukHEZug8N4lAxDggKT27_pgnKNnAu50qq3msJdf2nMizG2VzfHVTmwDsagnwdogrHw66pSQ", 
    "5cOVn9ZINUO4Ukd-PVmgtEZHqvu0PikPiUkMTrb6K8CzP5Fd-FWPxNVEWayj6EUQSF_ctmNWmqBVqg", 
    "1VvhvasECXxYfpFhkpNH-h-ZYVAn1ZD0YgGM49CW2u6q4o8ISveMwSz7JjRgt8njJptZLshg1WAj5g", 
    "fUWcifhm4SBGLeY45I4t2QY0I28Lk62mNtOhER3kZNuh9xeACXZznoBigQJX1DW-yWHTcz2jnXwb_g" 
]


API_KEY = os.getenv('API_KEY')

url_match_id = f'https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/' 
url_match_data = f'https://americas.api.riotgames.com/lol/match/v5/matches/'

headers = { 
    "X-Riot-Token": API_KEY
}

def match_data(url_match_id: str, url_match_data, puuids: list) -> list:
    latest_matches_id = []
    for  p in puuids:
        # extracting the id of latest match
        res_match_id = requests.get(url_match_id + p + '/ids?start=0&count=1', headers=headers)
        latest_matches_id.append(res_match_id.json()[0])
        
        if res_match_id.status_code != 200:
            logging.error('Erro na requisição de busca do match_id -> {status_code}')
            return []
        
        if not latest_matches_id:
            logging.warning('Nenhum match_id retornado')
            return []
        
    
    print(latest_matches_id)
    latest_matches_data = []
    for l in latest_matches_id:

        # extracting the data of latest match
        res_match_data = requests.get(url_match_data + l, headers=headers)
        latest_matches_data.append(res_match_data.json())
        
        if res_match_data.status_code != 200:
            logging.error('Erro na requisição de busca do match_data -> {status_code}')
            return []

        if not latest_matches_data:
            logging.warning('Nenhum dado de partida encontrado')
            return []

    # local de armazenamento dos dados
    output_path = 'data/latest_matches_data.json'
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:

       json.dump(latest_matches_data, f, indent=4)

    logging.info(f'Arquivo salvo com sucesso em: {output_path}')
    
    return []





    

import pandas as pd
from pathlib import Path
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

teams_cols_remove = [
        'bans',
        'objectives.atakhan.first',
        'objectives.atakhan.kills',
        'objectives.baron.first',
        'objectives.dragon.first',
        'objectives.horde.first',
        'objectives.inhibitor.first',
        'objectives.riftHerald.first',
        'feats.EPIC_MONSTER_KILL.featState',
        'feats.FIRST_BLOOD.featState',
        'feats.FIRST_TURRET.featState'
    ]  

teams_cols_rename = {
    'teamId': 'team_id',
    'win': 'win',
    'objectives.baron.kills': 'objectives_baron_kills',
    'objectives.champion.first': 'objectives_champion_first',
    'objectives.champion.kills': 'objectives_champion_kills',
    'objectives.dragon.kills': 'objectives_dragon_kills',
    'objectives.horde.kills': 'objectives_horde_kills',
    'objectives.inhibitor.kills': 'objectives_inhibitor_kills',
    'objectives.riftHerald.kills': 'objectives_rift_herald_kills',
    'objectives.tower.first': 'objectives_tower_first',
    'objectives.tower.kills': 'objectives_tower_kills',
    'match_id': 'match_id'
}

players_cols_remove = [
    'PlayerScore0'
                ,'PlayerScore1',
                'PlayerScore2',
                'PlayerScore3',
                'PlayerScore4',
                'PlayerScore5',
                'PlayerScore6',
                'PlayerScore7',
                'PlayerScore8',
                'PlayerScore9',
                'PlayerScore10',
                'PlayerScore11',
                'championId',
                'championTransform',
                'eligibleForProgression',
                'gameEndedInEarlySurrender',
                'gameEndedInSurrender',
                'item0',
                'item1',
                'item2',
                'item3',
                'item4',
                'item5',
                'item6',
                'lane',
                'nexusKills',
                'nexusLost',
                'nexusTakedowns',
                'participantId',
                'placement',
                'playerAugment1',
                'playerAugment2',
                'playerAugment3',
                'playerAugment4',
                'playerAugment5',
                'playerAugment6',
                'playerSubteamId',
                'profileIcon',
                'role',
                'spell1Casts',
                'spell2Casts',
                'spell3Casts',
                'spell4Casts',
                'subteamPlacement',
                'summoner1Casts',
                'summoner1Id',
                'summoner2Casts',
                'summoner2Id',
                'summonerId',
                'summonerLevel',
                'summonerName',
                'teamEarlySurrendered',
                'teamPosition',
                'timeCCingOthers',
                'unrealKills',
            ]

players_cols_rename =  {
    'allInPings': 'all_in_pings',
    'assistMePings': 'assist_me_pings',
    'assists': 'assists',
    'baronKills': 'baron_kills',
    'basicPings': 'basic_pings',
    'champExperience': 'champ_experience',
    'champLevel': 'champ_level',
    'championName': 'champion_name',
    'commandPings': 'command_pings',
    'consumablesPurchased': 'consumables_purchased',
    'damageDealtToBuildings': 'damage_dealt_to_buildings',
    'damageDealtToEpicMonsters': 'damage_dealt_to_epic_monsters',
    'damageDealtToObjectives': 'damage_dealt_to_objectives',
    'damageDealtToTurrets': 'damage_dealt_to_turrets',
    'damageSelfMitigated': 'damage_self_mitigated',
    'dangerPings': 'danger_pings',
    'deaths': 'deaths',
    'detectorWardsPlaced': 'detector_wards_placed',
    'doubleKills': 'double_kills',
    'dragonKills': 'dragon_kills',
    'enemyMissingPings': 'enemy_missing_pings',
    'enemyVisionPings': 'enemy_vision_pings',
    'firstBloodAssist': 'first_blood_assist',
    'firstBloodKill': 'first_blood_kill',
    'firstTowerAssist': 'first_tower_assist',
    'firstTowerKill': 'first_tower_kill',
    'getBackPings': 'get_back_pings',
    'goldEarned': 'gold_earned',
    'goldSpent': 'gold_spent',
    'holdPings': 'hold_pings',
    'individualPosition': 'individual_position',
    'inhibitorKills': 'inhibitor_kills',
    'inhibitorTakedowns': 'inhibitor_takedowns',
    'inhibitorsLost': 'inhibitors_lost',
    'itemsPurchased': 'items_purchased',
    'killingSprees': 'killing_sprees',
    'kills': 'kills',
    'largestCriticalStrike': 'largest_critical_strike',
    'largestKillingSpree': 'largest_killing_spree',
    'largestMultiKill': 'largest_multi_kill',
    'longestTimeSpentLiving': 'longest_time_spent_living',
    'magicDamageDealt': 'magic_damage_dealt',
    'magicDamageDealtToChampions': 'magic_damage_dealt_to_champions',
    'magicDamageTaken': 'magic_damage_taken',
    'needVisionPings': 'need_vision_pings',
    'neutralMinionsKilled': 'neutral_minions_killed',
    'objectivesStolen': 'objectives_stolen',
    'objectivesStolenAssists': 'objectives_stolen_assists',
    'onMyWayPings': 'on_my_way_pings',
    'pentaKills': 'penta_kills',
    'physicalDamageDealt': 'physical_damage_dealt',
    'physicalDamageDealtToChampions': 'physical_damage_dealt_to_champions',
    'physicalDamageTaken': 'physical_damage_taken',
    'pushPings': 'push_pings',
    'puuid': 'puuid',
    'quadraKills': 'quadra_kills',
    'retreatPings': 'retreat_pings',
    'riotIdGameName': 'riot_id_game_name',
    'riotIdTagline': 'riot_id_tagline',
    'roleBoundItem': 'role_bound_item',
    'sightWardsBoughtInGame': 'sight_wards_bought_in_game',
    'teamId': 'team_id',
    'timePlayed': 'time_played',
    'totalAllyJungleMinionsKilled': 'total_ally_jungle_minions_killed',
    'totalDamageDealt': 'total_damage_dealt',
    'totalDamageDealtToChampions': 'total_damage_dealt_to_champions',
    'totalDamageShieldedOnTeammates': 'total_damage_shielded_on_teammates',
    'totalDamageTaken': 'total_damage_taken',
    'totalEnemyJungleMinionsKilled': 'total_enemy_jungle_minions_killed',
    'totalHeal': 'total_heal',
    'totalHealsOnTeammates': 'total_heals_on_teammates',
    'totalMinionsKilled': 'total_minions_killed',
    'totalTimeCCDealt': 'total_time_cc_dealt',
    'totalTimeSpentDead': 'total_time_spent_dead',
    'totalUnitsHealed': 'total_units_healed',
    'tripleKills': 'triple_kills',
    'trueDamageDealt': 'true_damage_dealt',
    'trueDamageDealtToChampions': 'true_damage_dealt_to_champions',
    'trueDamageTaken': 'true_damage_taken',
    'turretKills': 'turret_kills',
    'turretTakedowns': 'turret_takedowns',
    'turretsLost': 'turrets_lost',
    'visionClearedPings': 'vision_cleared_pings',
    'visionScore': 'vision_score',
    'visionWardsBoughtInGame': 'vision_wards_bought_in_game',
    'wardsKilled': 'wards_killed',
    'wardsPlaced': 'wards_placed',
    'win': 'win',
    'match_id': 'match_id'
}

mastery_cols_rename = {
    'puuid': 'puuid',
    'championId': 'champion_id',
    'championLevel': 'champion_level',
    'championPoints': 'champion_points',
    'lastPlayTime': 'last_play_time',
    'championPointsSinceLastLevel': 'champion_points_since_last_level',
    'championPointsUntilNextLevel': 'champion_points_until_next_level'
}

path_matches = Path(__file__).resolve().parent.parent / 'data' / 'latest_matches_data.json' 
path_mastery = Path(__file__).resolve().parent.parent / 'data' / 'mastery_data.json'

# DATAFRAMES CREATIONS
def create_matches_dataframes(path_name: str) -> pd.DataFrame:
    
    logging.info("-> Criando Dataframe do arquivo JSON de partidas...")
    if not path_name.exists():
        raise FileNotFoundError(f"Arquivo JSON não encontrado: {path_name}")

    matches_list = []
    with open(path_name) as f:
        data = json.load(f)
    
    for d in data:
        matches_list.append(pd.json_normalize(d))

    df_matches = pd.concat(matches_list, ignore_index=True)

    logging.info(f"\n-> Dataframe 'Matches' criado com sucesso! \n {len(df_matches)} linhas detectadas")

    return df_matches

def create_teams_dataframe(df_matches: pd.DataFrame) -> pd.DataFrame:
    logging.info("-> Criando Dataframe dos times a partir do Dataframe de partidas..")

    # Teams dataframe
    teams_exploded = df_matches.explode('info.teams')
    df_teams = pd.json_normalize(teams_exploded['info.teams'])
    df_teams['match_id'] = teams_exploded['metadata.matchId'].values


    logging.info(f"\n-> Dataframe 'Teams' criado com sucesso! \n {len(df_teams)} linhas detectadas")
    return df_teams

def create_players_dataframe(df_matches: pd.DataFrame) -> pd.DataFrame:
    logging.info("-> Criando Dataframe dos jogadores a partir do Dataframe de partidas..")

    # Players dataframe
    players_exploded = df_matches.explode('info.participants') # 5 linhas, cada uma para um player 
    
    df_players = pd.json_normalize(players_exploded['info.participants'])

    df_players['match_id'] = players_exploded['metadata.matchId'].values

    logging.info(f"\n-> Dataframe 'Players' criado com sucesso! \n {len(df_players)}linhas detectadas")

    return df_players

def create_mastery_dataframe(path_name: str) -> pd.DataFrame:
    logging.info("-> Criando Dataframe do arquivo JSON de maestrias...")
    # Mastery dataframe
    mastery_list = []

    with open(path_name) as f:
        data = json.load(f)
    
    for d in data:
        mastery_list.append(pd.json_normalize(d))

    df_mastery = pd.concat(mastery_list, ignore_index=True)

    logging.info(f"\n-> Dataframe 'Mastery' criado com sucesso! \n {len(df_mastery)} linhas detectadas")

    return df_mastery

# DATAFRAMES TRANSFORMATIONS
def transform_matches_data(df_matches: pd.DataFrame) -> pd.DataFrame:
    # Criando um novo Dataframe de partidas somente com dados necessários 
    # a partir do Dataframe já existente
    logging.info("-> Transformando os dados de partida...")
    cols = {
        "metadata.matchId": "match_id",
        "info.gameCreation": "creation_time",
        "info.gameDuration": "duration",
        "info.gameEndTimestamp": "end_time"
    }

    new_df_matches = (
        df_matches[list(cols.keys())]
    )

    # Renomeando as colunas
    new_df_matches = new_df_matches.rename(columns=cols)

    # Normalizando datetimes do dataframe
    new_df_matches['creation_time'] = pd.to_datetime(new_df_matches['creation_time'], unit='ms',  utc=True).dt.tz_convert('America/Sao_Paulo')
    new_df_matches['end_time'] = pd.to_datetime(new_df_matches['end_time'], unit='ms',  utc=True).dt.tz_convert('America/Sao_Paulo')

    logging.info("-> Dados de partida transformados!")
    return new_df_matches

def transform_teams_data(df_teams: pd.DataFrame, cols_to_remove: list, cols_to_rename: dict) -> pd.DataFrame:
    logging.info("-> Transformando os dados de times...")
    # Remoção de colunas não importantes
    new_df_teams = df_teams.drop(columns=cols_to_remove)

    # Renomeando as colunas restantes
    new_df_teams = new_df_teams.rename(columns=cols_to_rename)
    logging.info("-> Dados dos times transformados!")
    return new_df_teams

def transform_players_data(df_players: pd.DataFrame, df_matches: pd.DataFrame, cols_to_remove: list, cols_to_rename:dict) -> pd.DataFrame:
    logging.info("-> Transformando os dados de jogadores...")
    prefixes = ('challenges.','missions','perks.')

    # Remove todas as colunas com os prefixos anotados
    new_df_players = df_players.loc[:, ~df_players.columns.str.startswith(prefixes)]

    # Remove as demais colunas sem prefixos específicos
    new_df_players = new_df_players.drop(columns=cols_to_remove)

    # Renomeando as colunas restantes
    new_df_players = new_df_players.rename(columns=cols_to_rename).reset_index(drop=True)

    logging.info("-> Dados de jogadores transformados!")
    return new_df_players

def transform_mastery_data(df_mastery: pd.DataFrame, cols_to_rename: dict) -> pd.DataFrame:
    logging.info("-> Transformando os dados de maestrias...")
    cols = [
        'markRequiredForNextLevel', 
        'tokensEarned', 
        'championSeasonMilestone', 
        'milestoneGrades'
        ]
    
    # Removendo colunas com o prefixo especificado
    new_df_mastery =  df_mastery.loc[:, ~df_mastery.columns.str.startswith('nextSeasonMilestone')] 
    # Removendo demais colunas sem prefixo específico
    new_df_mastery = new_df_mastery.drop(columns=cols)

    # Renomeando as colunas restantes
    new_df_mastery = new_df_mastery.rename(columns=cols_to_rename)
    new_df_mastery['last_play_time'] = pd.to_datetime(new_df_mastery['last_play_time'], unit='ms',  utc=True).dt.tz_convert('America/Sao_Paulo')
    logging.info("-> Dados de maestria transformados!")
    return new_df_mastery

def exec_transformations():

    logging.info("-> Inicializando as operações...")

    matches_df = create_matches_dataframes(path_name=path_matches)
    teams_df = create_teams_dataframe(matches_df)
    players_df = create_players_dataframe(matches_df)
    mastery_df = create_mastery_dataframe(path_name=path_mastery)

    matches_df = transform_matches_data(matches_df)
    teams_df = transform_teams_data(teams_df, cols_to_remove=teams_cols_remove, cols_to_rename=teams_cols_rename)
    players_df = transform_players_data(players_df, matches_df, cols_to_remove=players_cols_remove, cols_to_rename=players_cols_rename)

    mastery_df = transform_mastery_data(mastery_df, cols_to_rename=mastery_cols_rename)

    logging.info("-> Transformações concluídas!")
    
    return matches_df, teams_df, players_df, mastery_df

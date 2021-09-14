from io import StringIO
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from collections import defaultdict

import re
import boto3
import requests
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import pandas as pd
import numpy as np

AWS_ACCESS = "AKIATLIDN4DIMHCGZEHP"
AWS_SECRET_ID = "AE1AdRPh/asnS/MZcltETZgq0J5Nf0KTata/RzaR"
SERVICE = "s3"
REGION_NAME = "us-east-2"
BUCKET = "ykfbref"

S3 = boto3.resource(
    service_name=SERVICE,
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS,
    aws_secret_access_key=AWS_SECRET_ID
)
S3_BUCKET = S3.Bucket(BUCKET)

DATA_COLS = [
    "match_id",
    "team",
    "against",
    "h_a",
]

PLAYER_COLS = [
    "player",
    "shirtnumber",
    "nationality",
    "position",
    "age",
    "minutes"
]

SUM_COLS = [
    "goals",
    "assists",
    "pens_made",
    "pens_att",
    "shots_total",
    "shots_on_target",
    "cards_yellow",
    "cards_red",
    "touches",
    "pressures",
    "tackles",
    "interceptions",
    "blocks",
    "xg",
    "npxg",
    "xa",
    "sca",
    "gca",
    "passes_completed",
    "passes",
    "passes_pct",
    "progressive_passes",
    "carries",
    "progressive_carries",
    "dribbles_completed",
    "dribbles"
]

PASS_COLS = [
    "passes_completed",
    "passes",
    "passes_pct",
    "passes_total_distance",
    "passes_progressive_distance",
    "passes_completed_short",
    "passes_short",
    "passes_pct_short",
    "passes_completed_medium",
    "passes_medium",
    "passes_pct_medium",
    "passes_completed_long",
    "passes_long",
    "passes_pct_long",
    "assists",
    "xa",
    "assisted_shots",
    "passes_into_final_third",
    "passes_into_penalty_area",
    "crosses_into_penalty_area",
    "progressive_passes"
]

PASS_TYPE_COLS = [
    "passes",
    "passes_live",
    "passes_dead",
    "passes_free_kicks",
    "through_balls",
    "passes_pressure",
    "passes_switches",
    "crosses",
    "corner_kicks",
    "corner_kicks_in",
    "corner_kicks_out",
    "corner_kicks_straight",
    "passes_ground",
    "passes_low",
    "passes_high",
    "passes_left_foot",
    "passes_right_foot",
    "passes_head",
    "throw_ins",
    "passes_other_body",
    "passes_completed",
    "passes_offsides",
    "passes_oob",
    "passes_intercepted",
    "passes_blocked"
]

DEF_COLS = [
    "tackles",
    "tackles_won",
    "tackles_def_3rd",
    "tackles_mid_3rd",
    "tackles_att_3td",
    "dribble_tackles",
    "dribbles_vs",
    "dribble_tackles_pct",
    "dribble_past",
    "pressures",
    "pressure_regains",
    "pressure_regains_pct",
    "pressures_def_3rd",
    "pressures_mid_3rd",
    "pressures_att_3rd",
    "blocks",
    "blocked_shots",
    "blocked_shots_saves",
    "blocked_passes",
    "interceptions",
    "tackles_interceptions",
    "clearances",
    "errors"
]

POSS_COLS = [
    "touches",
    "touches_def_pen_area",
    "touches_def_3rd",
    "touches_mid_3d",
    "touches_att_3rd",
    "touches_att_pen_area",
    "touches_live_ball",
    "dribbles_completed",
    "dribbles",
    "dribbles_completed_pct",
    "players_dribbled_past",
    "nutmegs",
    "carries",
    "carry_distance",
    "carry_progressive_distance",
    "progressive_carries",
    "carries_into_final_third",
    "carries_into_penalty_area",
    "miscontrols",
    "dispossesed",
    "pass_targets",
    "passes_recieved",
    "passes_recieved_pct",
    "progressive_passes_recieved"
]

MISC_COLS = [
    "cards_yellow",
    "cards_red",
    "cards_yellow_red",
    "fouls",
    "fouled",
    "offsides",
    "crosses",
    "interceptions",
    "tackles_won",
    "pens_won",
    "pens_conceded",
    "own_goals",
    "ball_recoveries",
    "aerials_won",
    "aerials_lost",
    "aerials_won_pct"
]

KEEPER_COLS = [
    "player",
    "nationality",
    "age",
    "minutes",
    "shots_on_target_against",
    "goals_against_gk",
    "saves",
    "save_pct",
    "psxg_gk",
    "passes_completed_launched_gk",
    "passes_launched_gk"
    "passes_pct_launched_gk",
    "passes_gk",
    "passes_throws_gk",
    "pct_passes_launched_gk",
    "passes_length_avg_gk",
    "goal_kicks",
    "pcg_goal_kicks_launched",
    "goal_kick_length_avg",
    "crosses_gk",
    "crosses_stopped_gk",
    "crosses_stopped_pct_gk",
    "def_actions_outside_pen_area_gk",
    "avg_distance_def_actions_gk"
]

SHOT_COLS = [
    "minute",
    "player",
    "squad",
    "outcome",
    "distance",
    "body_part",
    "sca1_player",
    "sca1_event",
    "sca2_player",
    "sca2_event"
]

TABLE_COLS = [SUM_COLS, PASS_COLS, PASS_TYPE_COLS, DEF_COLS, POSS_COLS, MISC_COLS]

# s3 functions for help 
def s3_loaded_data(league_dir, directory):
    search_dir = league_dir + "/" + directory + "/"

    s3_checked = set([
        f.key.split(search_dir)[1] for f in S3_BUCKET.objects.filter(Prefix=search_dir).all()
        ]
    )

    return s3_checked

def process_keepers(home_table, away_table, home_team, away_team, match_id, league):
    pass

def process_shots(shots_table, match_id, league):
    pass

# helper for processing home and away data for players
def build_player_df(tables, match_id, team, against, h_a):
    df = pd.DataFrame()
    for i, t in enumerate(tables):
        table = t[:-1] # remove last row because it's an aggregated row
        table.columns = PLAYER_COLS + TABLE_COLS[i]

        if i == 0:
            df = table
            n = len(df)
            df['match_id'] = [match_id] * n
            df['team'] = [team] * n
            df['against'] = [against] * n
            df['h_a'] = [h_a] * n

            df = df[DATA_COLS + PLAYER_COLS + TABLE_COLS[i]]
        else:
            second_cols = table.columns.difference(df.columns)
            df = df.merge(
                table[second_cols],
                left_index=True,
                right_index=True,
                how='outer'
            )
    
    return df

def process_players(home_tables, away_tables, home_team, away_team, match_id, file_name):
    home_df = build_player_df(home_tables, match_id, home_team, away_team, 'home')
    away_df = build_player_df(away_tables, match_id, away_team, home_team, 'away')

    player_df = pd.concat([home_df, away_df], ignore_index=True)
    
    # upload to s3
    print('uploading... {} to S3'.format(file_name))
    csv_buffer = StringIO()
    player_df.to_csv(csv_buffer)
    S3.Object(BUCKET, file_name).put(Body=csv_buffer.getvalue())

def process_matches(match_data, league_dir):
    # check which matches have been already processed
    s3_players = s3_loaded_data(league_dir, 'player_data')
    s3_keepers = s3_loaded_data(league_dir, 'keeper_data')
    s3_shots = s3_loaded_data(league_dir, 'shot_data')

    for k, v in match_data.items():
        home = ''.join(v['squad_a'].split(' '))
        away = ''.join(v['squad_b'].split(' '))

        # file names for s3
        players_name = home + '_' + away + '_players.csv'
        keepers_name = home + '_' + away + '_keepers.csv'
        shots_name = home + '_' + away + '_shots.csv'

        # check if this file should be processed or not
        if (players_name in s3_players) and (keepers_name in s3_keepers) and (shots_name in s3_shots):
            continue

        # get game data
        game_tables = pd.read_html(v['match_link'])

        # process players
        process_players(
            game_tables[3:9],
            game_tables[10:16],
            home,
            away,
            v['match_id'],
            league_dir + '/player_data/' + players_name
        )
        break


def process_league(soup):
    season = soup.find('h2').find('span').text.split(' ')[0]
    competition = soup.find('h2').find('span').text.split(' ')[1:]
    competition = ' '.join(competition)

    match_table = soup.find('table', attrs={'class':'stats_table'}).find('tbody').find_all('tr')

    match_data = defaultdict(dict)
    match_num = 0

    for match in match_table:
        # check if this is a place holder row
        try: 
            if 'spacer' in match['class']:
                continue
        except:
            pass
        
        # get match metadata
        match_data[match_num]['season'] = season
        match_data[match_num]['competition'] = competition
        
        # check if the soup is bruck up
        try:
            match_data[match_num]['id_a'] = match.find_all('td', attrs={'data-stat':re.compile('squad')})[0].find('a')['href'].split('/')[3]
            match_data[match_num]['id_b'] = match.find_all('td', attrs={'data-stat':re.compile('squad')})[1].find('a')['href'].split('/')[3]
        except:
            del match_data[match_num]
            break
        
        # check if this match has been completed, if not, break and return data so far
        try:
            link = match.find('td', attrs={'data-stat':'score'}).find('a')['href']
            match_data[match_num]['match_link'] = 'https://fbref.com' + link
            match_data[match_num]['match_id'] = link.split('/')[3]
        except:
            del match_data[match_num]
            break

        for field in match:
            match_data[match_num][field['data-stat']] = field.text
            
        # find result
        try:
            score = match_data[match_num]['score']
            home = int(score[0])
            away = int(score[-1])
        except:
            # game is not finished yet, so break out
            del match_data[match_num]
            break
        
        result = 'a' if home > away else 'b'
        result = 'draw' if home == away else result
        
        # insert result
        match_data[match_num]['result'] = result
        
        match_num += 1

    return match_data

def main():
    # league details
    league_url = 'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures'
    league_dir = 'pl_2021_22'

    # webscrape match links
    options = Options()
    options.add_argument('--headless')

    driver = webdriver.Firefox(options=options)
    driver.get(league_url)
    html = driver.page_source
    soup = BeautifulSoup(html, 'lxml')
    driver.quit()
    print('Souped!')

    # get match details (links, ids, etc)
    match_data = process_league(soup)
    process_matches(match_data, league_dir, )


if __name__ == '__main__':
    main()
    
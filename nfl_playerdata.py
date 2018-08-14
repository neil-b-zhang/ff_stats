# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 19:23:22 2018

@author: neilb
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import sys

pd.options.mode.chained_assignment = None  # default='warn'

# constants
stat_categories = ['passing', 'rushing', 'receiving', 'defense', 'fumbles',
                   'kicking', 'punting', 'kickret', 'puntret']

# function for saving which weeks have been done to avoid extra scraping
# particularly for updates week to week vs whole seasons
def update_year_weeks(input_dict, year, week):
    # if year already in the dict, append it
    if year in input_dict:
        input_dict[year].append(week)
    
    # create key for week
    else:
        input_dict[year] = [week]
    return input_dict
    
# function for getting game ids by week
def get_game_ids(years, weeks, output_path):
    year_week_dict = {}
    for year in years: 
        season_dataframe = pd.DataFrame()
        for week in weeks:
            # use beautiful soup package to parse xml file containing weekly game ids and scores
            week_url = 'http://www.nfl.com/ajax/scorestrip?season=2017&seasonType=REG&week={}'.format(week)
            weekly_games_data = requests.get(week_url)
            weekly_games_data_soup=BeautifulSoup(weekly_games_data.text, 'xml')
            week_of_games = pd.DataFrame()
            
            # make dataframe out of individual game entries
            for game in weekly_games_data_soup.findAll('g'):
                game_attrs = dict(game.attrs)
                game_series = pd.Series(game_attrs)
                week_of_games = pd.concat([week_of_games, game_series], axis=1, sort=True)
                
            single_week_of_games = week_of_games.T
            
            # filter columns and add week # column
            columns_to_keep = ['d', 'eid', 'gsis', 'h', 't', 'hs', 'v', 'vs']
            single_week_of_games = single_week_of_games[columns_to_keep]
            single_week_of_games['week'] = week
            
            season_dataframe = pd.concat([season_dataframe, single_week_of_games])
            
            # call function that updates table that tracks existing weeks and years
            
            year_week_dict = update_year_weeks(year_week_dict, year, week)
        # end week loop
        # output weekly game ids for future records
        season_dataframe.to_csv(output_path + 'seasongames_{}.csv'.format(year), index = False)
    # end year loop
    year_week_df = pd.DataFrame(year_week_dict)
    year_week_df.to_csv('year_week_tracker.csv', index = False)

    
# gets and cleans player data for a single nfl game based on gameid
def get_game_data(years, output_path):
    for year in years:
        games_df = pd.read_csv(output_path + 'seasongames_{}.csv'.format(year))
        game_ids = games_df['eid'].unique().tolist()
        
        weeks = games_df['week'].unique().tolist()
        first_week = min(weeks)
        last_week = max(weeks)
        period_label = 'wks{}-{}'.format(first_week, last_week)
        
        for stat_category in stat_categories:
            print('currently getting {} data...'.format(stat_category))
            stats_by_category = pd.DataFrame()

            for game_id in game_ids:
                data_by_gameid_url = 'http://www.nfl.com/liveupdate/game-center/{}/{}_gtd.json'.format(game_id, game_id)
                data_by_gameid_json = requests.get(data_by_gameid_url)
            
                data = json.loads(data_by_gameid_json.text)
                
                home_away_df = pd.DataFrame()
                # parse json data to get player data
                both_skipped = True
                try:
                    for player_id in data[str(game_id)]['home']['stats'][stat_category]:
                        home_df = pd.DataFrame(list(data[str(game_id)]['home']['stats'][stat_category][player_id].items()))
                        home_df.set_index(home_df.columns.tolist()[0], inplace=True)
                        home_df = home_df.T
                        home_df['player_id'] = player_id
                        home_away_df = pd.concat([home_away_df, home_df])
                        both_skipped = False
                except KeyError:
                    print('{} not found in game {} for {} team'.format(stat_category, game_id, 'home'))
                try: 
                    for player_id in data[str(game_id)]['away']['stats'][stat_category]:
                        away_df = pd.DataFrame(list(data[str(game_id)]['away']['stats'][stat_category][player_id].items()))
                        away_df.set_index(away_df.columns.tolist()[0], inplace=True)
                        away_df = away_df.T
                        away_df['player_id'] = player_id
                        home_away_df = pd.concat([home_away_df, away_df])
                        both_skipped = False
                except KeyError:
                    print('{} not found in game {} for {} team'.format(stat_category, game_id, 'away'))
                
                if not both_skipped:
                    rename_cols = home_away_df.columns.tolist()
                    rename_cols.remove('name')
                    rename_cols.remove('player_id')

                    home_away_df_noname = home_away_df[rename_cols]    
                    home_away_df_noname.columns = ['{}_'.format(stat_category) + str(col) for col in home_away_df_noname.columns]
                    home_away_df_noname['game_id'] = game_id
                    home_away_df = pd.concat([home_away_df[['name','player_id']], home_away_df_noname], axis = 1)

                    stats_by_category = pd.concat([stats_by_category, home_away_df])
            
            # end game_id loop
            stats_by_category.to_csv(output_path + '{}_{}_{}.csv'.format(stat_category, year, period_label),
                                     index = False)
        # end stat category loop
    # end year loop
            
def main():
    output_path = 'C:/Users/neilb/Documents/FF/data/'
    
    years = [2017]
    weeks = list(range(1,18))

    
    get_game_ids(years, weeks, output_path)
    get_game_data(years, output_path)     
    
    
if __name__ == "__main__":
    main()
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 21:23:22 2018

@author: neilb
"""
import pandas as pd
import glob

def merge_datasets(data_folder):
    final_dataframe = pd.DataFrame()
    
    # count to track which merge currently on
    count = 0
    for file in glob.glob(data_folder + "*.csv"):
        if file != 'C:/Users/neilb/Documents/FF/data\seasongames_2017.csv' and file != 'C:/Users/neilb/Documents/FF/data\player_data.csv':
            print(file)
            temp_stats = pd.read_csv(file)
            try:
                final_dataframe = pd.merge(final_dataframe, temp_stats, 
                                           on = ['game_id', 'name', 'player_id'],
                                           how = 'outer')
            except KeyError:
                final_dataframe = temp_stats
            count += 1
            #final_dataframe.to_csv('final_{}.csv'.format(count), index = False)
    final_dataframe = final_dataframe.fillna(0)
    
    return final_dataframe

def clean_player_data(fsd, player_data, data_folder):
    print(player_data.columns.tolist())
    # calculate some additional fields
    player_data['tot_touchdowns'] = player_data['receiving_tds'] + player_data['rushing_tds'] + player_data['kickret_tds'] + player_data['puntret_tds']
    player_data['fantasy_points'] = player_data['passing_yds']*fsd['pp_passingyrd']\
                                    + player_data['passing_tds']*fsd['passing_tds']\
                                    + player_data['passing_twoptm']*2\
                                    + player_data['receiving_rec']*fsd['ppr']\
                                    + player_data['receiving_yds']*fsd['pp_rec_rushyrd']\
                                    + player_data['receiving_twoptm']*2\
                                    + player_data['rushing_yds']*fsd['pp_rec_rushyrd']\
                                    + player_data['rushing_twoptm']*2\
                                    + player_data['tot_touchdowns']*fsd['rec_rush_ret_tds']\
                                    + player_data['fumbles_lost']*fsd['lost_fumble']\
                                    + player_data['passing_ints']*fsd['interception']
                                    
    
    player_data.to_csv(data_folder + 'player_data.csv', index = False)
    return player_data

def main():
    data_folder = 'C:/Users/neilb/Documents/FF/data/'
    ppr = 0.5
    pp_passingyrd = 0.04
    pp_rec_rushyrd = 0.1
    passing_tds = 4
    rec_rush_ret_tds = 6
    interception = -1
    lost_fumble = -2
    
    # create settings dict
    fantasy_settings_dict = {}
    fantasy_settings_dict['ppr'] = ppr
    fantasy_settings_dict['pp_passingyrd'] = pp_passingyrd
    fantasy_settings_dict['pp_rec_rushyrd'] = pp_rec_rushyrd
    fantasy_settings_dict['passing_tds'] = passing_tds
    fantasy_settings_dict['rec_rush_ret_tds'] = rec_rush_ret_tds
    fantasy_settings_dict['interception'] = interception
    fantasy_settings_dict['lost_fumble'] = lost_fumble
    
    player_data = merge_datasets(data_folder)
    cleaned_dataset = clean_player_data(fantasy_settings_dict, player_data, data_folder)
    
    
if __name__ == "__main__":
    main()
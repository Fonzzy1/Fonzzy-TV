import datetime
import os
import datetime
from moviepy.editor import VideoFileClip
import pandas as pd
import shutil
import sqlalchemy


def assign_dir():
    try:
        directory =  input('What is the location of the directory with the shows in?')

        os.chdir(directory)

        subfolders = [f.name for f in os.scandir(directory) if f.is_dir()]
        for folder in subfolders:
            print(folder)
        r = input('Are these the shows?(Y/n)')
        if r.lower() != 'y':
            assign_dir()
        else:
            return (directory, subfolders)
    except:
        print('That is not a correct directory')
        assign_dir()


def build_channels():
    print('Now building SQL database for channels and scheduling')
    df_channel = pd.DataFrame(columns=['channel_name'])
    channel_name = input('What would you like to call channel no.' + str(len(df_channel)))
    while channel_name != '':
        df_channel.loc[len(df_channel)] = [channel_name]
        channel_name = input('What would you like to call channel no.' + str(len(df_channel)) +
                             '? (Leave blank to stop adding channels)')

    return df_channel


def setup():
    file_loc = os.path.dirname(os.path.realpath(__file__))
    df_config = pd.DataFrame(columns=['key', 'value'])
    df_config.loc[0] = ['script_loc', file_loc]

    print('Welcome to Fonzzy TV \n'
          'Please ensure an instance of MySQL is loaded on this machine,'
          'and ensure the requirements in requirements.txt are met  \n'
          '\n'
          'This  script will format the files that will be displayed,'
          ' update the config file,'
          ' and set up the MySQL backend.\n'
          )
    resposne = input('Do you want to continue(Y/n)')
    if resposne.lower() != 'y':
        exit(1)

    (directory, show_list) = assign_dir()
    df_config.loc[1] = ['file_dir', directory]

    backup = input('Do you want to back up the directory before reformatting?'
                   ' Note this may take a while and use up a lot of disk space(Y/n)')
    if backup.lower() == 'y':
        shutil.copytree(directory, os.path.dirname(os.getcwd()) + '/Fonzzy-TV-Backup')

    show_names = input('Are the file names the names of the shows that you want to be stored?(Y/n)')
    if show_names.lower() != 'y':
        for file in show_list:
            show_name = input('What show is ' + file)
            os.rename('./' + file, './' + show_name)
    show_list = [f.name for f in os.scandir(directory) if f.is_dir()]

    in_order = input('Are series seperated in sub folders in alphabetical order,'
                     'and are moveis and specials not in sub folders (Y/n)')
    if in_order.lower() != 'y':
        cont = input('Please go to your file manager and organise this, press enter to continue')

    print('Renaming and formatting folders')

    for show in show_list:
        os.chdir(directory + '/' + show)
        snm_list = sorted([f for f in os.listdir(os.getcwd()) if os.path.isfile(os.path.join(os.getcwd(), f))],
                          key=str.lower)

        for snm in snm_list:
            os.rename(snm, 'M' + str(snm_list.index(snm) + 1).zfill(2))

        series_list = sorted([f.name for f in os.scandir(os.getcwd()) if f.is_dir()], key=str.lower)

        for series in series_list:
            os.rename(series, 'S' + str(series_list.index(series) + 1).zfill(2))

        series_list = sorted([f.name for f in os.scandir(os.getcwd()) if f.is_dir()], key=str.lower)

        for series in series_list:
            os.chdir(directory + '/' + show + '/' + series)
            episode_list = sorted([f for f in os.listdir(os.getcwd()) if os.path.isfile(os.path.join(os.getcwd(), f))],
                                  key=str.lower)
            for episode in episode_list:
                os.rename(episode, 'E' + str(episode_list.index(episode) + 1).zfill(2))

    df_channels = build_channels()

    print(df_channels)

    df_shows = pd.DataFrame(columns=['show_name', 'channel_id'])
    for show in show_list:
        channel = input('What is the index of the channel for ' + show)
        df_shows.loc[len(df_shows)] = [show, channel]

    df_series = pd.DataFrame(columns=['show_name', 'series_name'])
    df_episodes = pd.DataFrame(columns=['show_name', 'series_name', 'episode_name', 'path', 'duration'])

    print('Building Tables')
    for show in show_list:
        series_list = [f.name for f in os.scandir(directory + '/' + show) if f.is_dir()]
        for series in series_list:
            df_series.loc[len(df_series)] = [show, series]
            episode_list = [f for f in os.listdir(directory + '/' + show + '/' + series) if
                            os.path.isfile(os.path.join(directory + '/' + show + '/' + series, f))]
            for episode in episode_list:
                show_len = VideoFileClip(directory + '/' + show + '/' + series + '/' + episode).duration
                df_episodes.loc[len(df_episodes)] = [show, series, episode,
                                                     directory + '/' + show + '/' + series + '/' + episode, show_len]

        snm_list = [f for f in os.listdir(directory + '/' + show) if
                    os.path.isfile(os.path.join(directory + '/' + show, f))]
        for snm in snm_list:
            show_len = VideoFileClip(directory + '/' + show + '/' + snm).duration
            df_episodes.loc[len(df_episodes)] = [show, None, snm, directory + '/' + snm, show_len]

    df_channels['last_date'] = datetime.datetime.now()

    sql_username = input('SQL username (username and password stored in plain text, dont give the root user): ')
    sql_password = input('SQL Password')

    df_config.loc[2] = ['sql_username', sql_username]
    df_config.loc[3] = ['sql_password', sql_password]

    db_connection_str = 'mysql+pymysql://' + sql_username + ':' + sql_password + '@localhost'
    engine = sqlalchemy.create_engine(db_connection_str)
    conn = engine.connect()

    engine.execute('drop database if exists fonzzy_tv')
    engine.execute('create database fonzzy_tv')

    df_channels.to_sql('tbl_channels', conn, 'fonzzy_tv')
    df_shows.to_sql('tbl_shows', conn, 'fonzzy_tv')
    df_series.to_sql('tbl_series', conn, 'fonzzy_tv')
    df_episodes.to_sql('tbl_episodes', conn, 'fonzzy_tv')

    df_config.to_csv(file_loc + '/config.csv')

    return

import datetime
import os
import sys

import start
import build_schedule
import time
import pandas as pd
import sqlalchemy



def play_loop(channel_id, conn):
    df_current = pd.read_sql_query('select m.channel_id,'
                                   ' concat(show_name, \' - \',series_name,episode_name) as episode,'
                                   ' path as path,'
                                   ' timestampdiff(second,  s.start_time, now()) as cur_time,'
                                   ' s.duration'
                                   ' from tbl_schedule s '
                                   'join (select channel_id , max(start_time) as start_time '
                                   'from tbl_schedule where start_time <= now() group by channel_id) m '
                                   'on s.channel_id = m.channel_id and s.start_time = m.start_time '
                                   'order by channel_id', conn)

    path = df_current['path'][channel_id]
    start_time = df_current['cur_time'][channel_id]
    remaining_time = df_current['duration'][channel_id] - start_time + 1
    # creating vlc media player object

    # start playing video
    cmd = 'cvlc  --start-time=' + str(start_time) + ' \'' + path + '\' >/dev/null 2>&1'
    os.system(cmd)

    time.sleep(remaining_time)
    play_loop(channel_id)


def main():
    os.chdir(os.path.dirname(sys.argv[0]))
    try:
        df_config = pd.read_csv('./config.csv')
        sql_username = df_config.iloc[2, 2]
        sql_password = df_config.iloc[3, 2]

        db_connection_str = 'mysql+pymysql://' + sql_username + ':' + sql_password + '@localhost/fonzzy_tv'
        engine = sqlalchemy.create_engine(db_connection_str)
        conn = engine.connect()

        df_channels = pd.read_sql_query('select * from tbl_channels', conn)
    except:
        start.setup()

        df_config = pd.read_csv('./config.csv')
        sql_username = df_config.iloc[2, 2]
        sql_password = df_config.iloc[3, 2]

        db_connection_str = 'mysql+pymysql://' + sql_username + ':' + sql_password + '@localhost/fonzzy_tv'
        engine = sqlalchemy.create_engine(db_connection_str)
        conn = engine.connect()

        df_channels = pd.read_sql_query('select * from tbl_channels', conn)

    min_date = pd.read_sql_query('select min(last_date) as date from tbl_channels',conn)
    if min_date['date'][0] < datetime.datetime.now():
        build_schedule.main()

    os.system('clear')
    print(df_channels[df_channels.columns[1:2]])
    channel_index = int(input('What Channel do you want to play?'))

    play_loop(channel_index, conn)



if __name__ == "__main__":
    main()
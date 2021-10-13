import datetime
import os
import sys

import pyfiglet

from player import play_loop, Interrupt
import start
import build_schedule
import time
import pandas as pd
import sqlalchemy

class InterruptExecution (Exception):
    pass

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

    min_date = pd.read_sql_query('select min(last_date) as date from tbl_channels', conn)
    if min_date['date'][0] < datetime.datetime.now():
        build_schedule.main()

    channel_index = 0
    while channel_index in range(0, len(df_channels)):
        os.system('clear')
        pyfiglet.print_figlet('Fonzzy-TV', colors= "RED")

        print(df_channels[df_channels.columns[1:2]])
        channel_index = int(input('What Channel do you want to play?'))

        play_loop(channel_index, conn)




if __name__ == "__main__":
    main()
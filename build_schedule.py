import datetime
import pandas as pd
import sqlalchemy

def main():
    last_monday = datetime.datetime.combine(
        datetime.date.today() - datetime.timedelta(days=datetime.date.today().weekday()), datetime.time())

    df_config = pd.read_csv('./config.csv')
    sql_username = df_config.iloc[2, 2]
    sql_password = df_config.iloc[3, 2]

    db_connection_str = 'mysql+pymysql://' + sql_username + ':' + sql_password + '@localhost/fonzzy_tv'
    engine = sqlalchemy.create_engine(db_connection_str)
    conn = engine.connect()

    df_channels = engine.execute('select * from tbl_channels').fetchall()

    df_schedule = pd.DataFrame(columns=['channel_id', 'show_name', 'series_name', 'episode_name', 'path',
                                        'duration', 'start_time'])

    for (channel_index, channel_name, last_date) in df_channels:
        df_channel_shows = pd.read_sql_query('select s.channel_id, '
                                             's.show_name, '
                                             'series_name, '
                                             'episode_name, '
                                             'path, '
                                             'duration '
                                             'from tbl_episodes e '
                                             'join tbl_shows s on e.show_name = s.show_name '
                                             'where s.channel_id = ' + str(channel_index) + ' ORDER BY RAND ();', conn)
        df_channel_shows['start_time'] = None
        df_channel_shows['start_time'][0] = last_monday
        for row in range(1, len(df_channel_shows)):
            df_channel_shows['start_time'][row] = df_channel_shows['start_time'][row - 1] + datetime.timedelta(
                seconds=float(df_channel_shows['duration'][row - 1]))
        end_time = max(df_channel_shows['start_time'])
        engine.execute('update tbl_channels set last_date = \''+ str(end_time) + '\' where `index` = ' + str(channel_index) +';')
        df_schedule = df_schedule.append(df_channel_shows)

    df_schedule.to_sql('tbl_schedule', conn, if_exists='replace' , index = False)

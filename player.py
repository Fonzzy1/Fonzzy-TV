import os
import subprocess
import time
import pandas as pd


class Interrupt(Exception):
    pass


def play_loop(channel_id, conn):
    try:
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
        cmd = ['cvlc', '--start-time=' + str(start_time), path, '&']
        player = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        time.sleep(remaining_time)
        player.terminate()

        play_loop(channel_id, conn)
    except:
        player.terminate()
        return

"""
Created on Tue Jun 21 15:46:44 2016

@author: matt_k
"""


import os
import pandas as pd
import numpy as np

os.chdir('/home/matt/Semios/ML-master')

import psycopg2
import degreeday.fetchfromSQL as fetch
import datetime as dt
import matplotlib.pyplot as plt
import scipy as sp


state = 'WA'
node_type = 'scd'
data_start = '2015-06-01 12:00:00'
timezone = 'America/Los_Angeles'

#prop = "Peterson Ranch"
prop = "Hyland - TIC"
#prop = "Fresno Warehouse"
#prop = "SunWest Fruit - Kazarian Ranch"
#prop = "O'Neill Agri-Management - Mersal Farm"
#prop = "JSA Farms"
#prop = "Turk Station"

date_UTC = pd.Timestamp(data_start, tz = timezone).tz_convert('UTC')

subplots = 20

mac_dict = {'001C2C1B2660E91C' : 'Test 1: 3 min, full can', '001C2C1B2660E928' : 'Test 2: 3 min, full can', 
            '001C2C1B2660E9EB' : 'Test 3: 3 min, full can', '001C2C1B2660ECCA' : 'Test 4: 3 min, half can', 
            '001C2C1B2660F108' : 'Test 5: 3 min, half can', '001C2C1B2660F146' : 'Test 6: 3 min, half can',
            '001C2C1B2660F244' : 'Test 7: 3 min, no can', '001C2C1B2660F267' : 'Test 8: 3 min, no can', 
            '001C2C1B2660F50E' : 'Test 9: 3 min, no can', '001C2C1B2660F8F8' : 'Test 10: No schedule', 
            '001C2C1B2660E84B' : 'Test 17: 9 min, no can', '001C2C1B2660E92C' : 'Test 18: 9 min, no can', 
            '001C2C1B2660E968' : 'Test 19: 3 min, Energizers', '001C2C1B2660EA9B' : 'Test 20: 3 min, Energizers', 
            '001C2C1B2660ECF9' : 'Test 21: 3 min, Energizers', '001C2C1B2660EF9A' : 'Test 22: 3 min, Energizers, inside',
            '001C2C1B2660EFDF' : 'Test 23: 3 min, Energizers, inside', '001C2C1B2660F0B7' : 'Test 24: 3 min, Energizers, inside',
            '001C2C1B2660F90F' : 'Test 25:  3 min, no can', '001C2C1B2660F94F' : 'Test 26:  3 min, no can',
            '001C2C1B2660F996' : 'Test 27:  3 min, no can'}


readprod = {
      "type": "postgresql",
      "data_source_provider": "rds",
      "adapter": "",
      "database": "prod",
      "encoding": "utf8",
      "host": "prod-restored.c2p0z5xffb9q.us-west-2.rds.amazonaws.com",
      "username": "readonly",
      "password": "a90c1715e7866b8c1c61fb49cd7abd37",
      "reconnect": "true"
}

conn = fetch.get_psycopg2_connect(db_config = readprod)


if prop == 'Fresno Warehouse':
    query = (
            "SELECT l.battery as bat, l.mac, l.stamp, l.ack, l.spray_count_0, s.name "
            "FROM log.{} l "
            "LEFT JOIN LATERAL ( "
                "SELECT DISTINCT(n.mac), p.name "
                "FROM node.{} n "
                "JOIN properties p on p.id = n.property_id "
                "WHERE p.name = '{}' "
                "AND n.mac IN ('{}') " # For plotting Fresno
                "LIMIT {} ) s on TRUE "
            "WHERE l.stamp > '{}' "
            "AND l.mac = s.mac "
            "ORDER BY l.stamp ASC "
            "".format(node_type, node_type, 
                      prop, 
                      ("', '".join(str(x) for x in mac_dict.keys())), 
                      subplots, date_UTC)
            )
elif prop == "O'Neill Agri-Management - Mersal Farm":
    query = (
            "SELECT l.battery as bat, l.mac, l.stamp, l.ack, l.spray_count_0, s.name "
            "FROM log.{} l "
            "LEFT JOIN LATERAL ( "
                "SELECT DISTINCT(n.mac), p.name "
                "FROM node.{} n "
                "JOIN properties p on p.id = n.property_id "
                "WHERE p.name like '%Neill Agri-Management - Mersal%' " # For plotting Mersal
                "LIMIT {} ) s on TRUE "
            "WHERE l.stamp > '{}' "
            "AND l.mac = s.mac "
            "ORDER BY l.stamp ASC "
            "".format(node_type, node_type, 

                      subplots, date_UTC)
            )
else:
    query = (
            "SELECT l.battery as bat, l.mac, l.stamp, l.ack, l.spray_count_0, s.name "
            "FROM log.{} l "
            "LEFT JOIN LATERAL ( "
                "SELECT DISTINCT(n.mac), p.name "
                "FROM node.{} n "
                "JOIN properties p on p.id = n.property_id "
                "WHERE p.name = '{}' "
                "LIMIT {} ) s on TRUE "
            "WHERE l.stamp > '{}' "
            "AND l.mac = s.mac "
            "ORDER BY l.stamp ASC "
            "".format(node_type, node_type, prop, subplots, date_UTC)
            )


bat_df = pd.read_sql(query, conn)
macs = bat_df.mac.unique()

#test_query = ( 
#             "SELECT "
#             "p.name AS property_name, "
#             "min(log.stamp) as stamp "
#             "FROM node.scd s "
#             "JOIN properties p ON p.id = s.property_id "
#             "JOIN LATERAL ( "  
#                "SELECT "
#                "stamp, "
#                "app_version "
#                "FROM log.node_pings "
#                "WHERE app_version LIKE '04.15.10' "
#                "AND mac = s.mac "
#                "ORDER BY stamp ASC "
#                "LIMIT 1 ) log ON TRUE "
#             "GROUP BY property_name "
#             "ORDER BY stamp ASC; "
#             "".format(("', '".join(str(x) for x in macs)), date_UTC)
#             )
#
#test_df = pd.read_sql(test_query, conn)

ping_query = ( 
             "SELECT ping.stamp, ping.app_version as fw, ping.mac "
             "FROM log.node_pings ping "
             "WHERE ping.mac IN ('{}') "
#             "AND ping.stamp < '{}' "
             "ORDER BY ping.stamp ASC "
             "".format(("', '".join(str(x) for x in macs)))
             )

ping_df = pd.read_sql(ping_query, conn)


schedule_query = (
                 "SELECT sch.start_ts, sch.frequency, sch.node_mac as mac "
                 "FROM schedule_history sch "
                 "WHERE sch.node_mac in ('{}') "
                 "AND sch.start_ts > '{}' "
                 "ORDER BY sch.start_ts ASC "
                 "".format(("', '".join(str(x) for x in macs)), date_UTC)
                 )
                 
schedule_df = pd.read_sql(schedule_query, conn)

print "Date range is {} to {}".format(min(bat_df.stamp), max(bat_df.stamp))

for i in macs:
    print 'For mac: ' + i + '\n Schedule : \n', schedule_df.loc[schedule_df.mac == i]
    print '\n Node pings: \n', ping_df.loc[ping_df.mac == i]
    print '\n\n\n'
    
def date_to_time(t, t_0):
    return (t - t_0).total_seconds()

def sch_upgrade(sch_now, sch_before):
    if sch_now == sch_before:
        return False
    else:
        return True

def ping_is_update(fw_now, fw_before):
    if fw_now == fw_before:
        return False
    else:
        return True

def is_series_sorted(ser):
    return (ser.diff().fillna(0) >= 0).all()

def fix_sprays(df):
    ser = df.spray_count_0
    start_spray = ser.iloc[0]
    ser = ser - start_spray
    ser = ser.reset_index()
    diff = ser.diff().fillna(0)
    reset = diff.spray_count_0.loc[diff.spray_count_0 < 0].index
    reset_values = [(ser.spray_count_0.iloc[x-1] + start_spray) for x in reset]
    reset_dates = [ser['index'].iloc[x-1] for x in reset]
    for i in range(len(reset)):
        ser.loc[ser.index >= reset[i], 'spray_count_0'] = ser.loc[ser.index >= reset[i], 'spray_count_0'] + reset_values[i]
    ser.set_index(ser['index'], inplace = True)
    del ser['index']
    if len(reset > 0):
        return ser, df.stamp.iloc[reset], len(reset)
    else:
        return ser, ['None'], 0


for i in range(subplots):
    fig = plt.figure(figsize = (13, 8))

    ax = fig.add_subplot(1,1,1)
    mac_df = bat_df.loc[bat_df.mac == macs[i]]
    ax.scatter(mac_df.stamp.values, mac_df.bat.values)
    
    t_0 = mac_df.stamp.iloc[0]
    t_1 = mac_df.stamp.iloc[-1]
    
    sch_sub = schedule_df.loc[schedule_df.mac == macs[i]]
    top_sch_df = 1
    sch_chg_ls = 'dashed'
    sch_chg_lw = 3
    sch_chg_col = iter(plt.cm.rainbow(np.linspace(0,1,len(sch_sub))))
    sch_same_ls = '-.'
    sch_same_lw = 0.8
    sch_same_alpha = 0.5
    for j in range(len(sch_sub)):
        if sch_sub.start_ts.iloc[j] >= t_0:
            if top_sch_df == 0:
                if sch_upgrade(sch_sub.frequency.iloc[j], sch_sub.frequency.iloc[j-1]):
                    c = next(sch_chg_col)
                    ax.axvline(x = sch_sub.start_ts.iloc[j], color = c, 
                               label = 'Schedule changed: freq = ' + str(sch_sub.frequency.iloc[j]), 
                               ls = 'dashed', lw = sch_chg_lw)
                else:
                    ax.axvline(x = sch_sub.start_ts.iloc[j], color = c, 
                               ls = 'dashed', alpha = sch_same_alpha, lw = sch_same_lw)
            else:
                c = next(sch_chg_col)
                ax.axvline(x = sch_sub.start_ts.iloc[j], color = c, 
                           label = 'Beginning Schedule = ' + str(sch_sub.frequency.iloc[j]), 
                           ls = 'dashed', lw = sch_chg_lw)                
        elif sch_sub.start_ts.iloc[j] < t_0 and top_sch_df == 1:
            c = next(sch_chg_col)
            ax.axvline(x = t_0, color = c, label = "Schedule starts at: " + 
                       str(sch_sub.loc[sch_sub.start_ts <= t_0].frequency.iloc[-1]), 
                       ls = 'dashed', lw = sch_chg_lw)
        top_sch_df = 0
        

    ping_sub = ping_df.loc[ping_df.mac == macs[i]]
    top_ping_df = 1
    ping_ls = '-.'
    ping_lw = 0.8
    ping_alpha = 0.5
    ping_col = 'g'
    fw_up_ls = 'dashdot'
    fw_up_lw = 3
    fw_up_col = iter(plt.cm.rainbow(np.linspace(0,1,len(ping_sub.fw.unique()))))
    for k in range(len(ping_sub)):
        if ping_sub.stamp.iloc[k] > t_0:
            if top_ping_df == 0:
                if ping_is_update(ping_sub.fw.iloc[k], ping_sub.fw.iloc[k-1]):
                    ax.axvline(x = ping_sub.stamp.iloc[k], color = next(fw_up_col), 
                               label = "Firmware update from : " + str(ping_sub.fw.iloc[k-1]) + " to " + 
                               str(ping_sub.fw.iloc[k]), ls = fw_up_ls, lw = fw_up_lw)
                else:
                    ax.axvline(x = ping_sub.stamp.iloc[k], 
                               color = ping_col, ls = ping_ls, alpha = ping_alpha, lw = ping_lw)
            else:
                ax.axvline(x = ping_sub.stamp.iloc[k], color = next(fw_up_col), 
                           label = "First node ping, firmware is : " + 
                           str(ping_sub.fw.iloc[k]), ls = fw_up_ls, lw = fw_up_lw)
        elif top_ping_df == 1:
            ax.axvline(x = t_0, color = next(fw_up_col), label = "Firmware starts at: " + 
                       str(ping_sub.loc[ping_sub.stamp <= t_0].fw.iloc[-1]), ls = fw_up_ls, lw = fw_up_lw)
        top_ping_df = 0
        
    
    mac_df['spray_count_0'], resets, num_resets = fix_sprays(mac_df.loc[:, ['spray_count_0', 'stamp']])
    spr_col = iter(plt.cm.rainbow(np.linspace(0,1,len(mac_df.loc[mac_df.bat > 2.1, 'bat'].unique()))))
    last_sprays = 0
    last_V = 'Starting '
    last_time = t_0
    for l in np.sort(mac_df.bat.unique())[::-1]:
        if l > 2.1 and (t_1 - max(mac_df.loc[mac_df.bat == l, 'stamp'])) > pd.Timedelta('1 days') and len(mac_df.loc[mac_df.bat == l]) > 40:
            time = mac_df.loc[mac_df.bat == l, 'stamp'].iloc[-1]
            sprays = mac_df.loc[mac_df.bat == l, 'spray_count_0'].iloc[-1]
            ax.axvline(x = time, color = next(spr_col), label = str(sprays - last_sprays) + ' sprays from: ' +
                       str(last_V) + 'V -> ' + str(l-0.1) + ' , days: ' + str((time - last_time).days))
            last_time = time
            last_sprays = sprays
            last_V = l - 0.1

            
    major_ticks = np.arange(min(mac_df.bat), max(mac_df.bat), 0.3)
    minor_ticks = np.arange(min(mac_df.bat), max(mac_df.bat), 0.1)
    ax.set_yticks(major_ticks)
    ax.set_yticks(minor_ticks, minor = True)
    ax.legend(loc = 'lower left', fontsize = 8)
    ax.xaxis.set_label_text('Date')
    ax.yaxis.set_label_text('Voltage')
    if prop == 'Fresno Warehouse':
        ax.set_title(str(i) + ': Node mac: ' + macs[i] + ", " + mac_dict[macs[i]] + ', ' + str(num_resets) + ' spray resets, some dates: ' + ', '.join(str(e.date()) if type(e) != type('h') else e for e in resets[:2]))
    else:
        ax.set_title(str(i) + ': Node mac: ' + macs[i] + ", Property: " + prop + ', ' + str(num_resets) + ' spray resets, some dates: ' + ', '.join(str(e.date()) if type(e) != type('h') else e for e in resets[:2]))
    ax.yaxis.grid(which = 'both')
    
i = [0, 2, 4, 6, 7, 11]
v = 3.1

des_v(i, v)

def des_v(mac_ind, v):
    spray_counter = []
    day_counter = []
    for i in mac_ind:
        mac_df = bat_df.loc[bat_df.mac == macs[i]]
        mac_df['spray_count_0'], a, b = fix_sprays(mac_df.loc[:, ['spray_count_0', 'stamp']])
        last_sprays = mac_df.loc[mac_df.bat == (v + 0.1), 'spray_count_0'].iloc[-1]
        sprays = mac_df.loc[mac_df.bat == v, 'spray_count_0'].iloc[-1]
        spray_counter.append(sprays - last_sprays)
        last_days = mac_df.loc[mac_df.bat == (v + 0.1), 'stamp'].iloc[-1]
        days = mac_df.loc[mac_df.bat == v, 'stamp'].iloc[-1]
        day_counter.append((days - last_days).days)
    print 'sprays ', spray_counter, ' days ', day_counter
    print 'spray std: ', sp.std(spray_counter)
    print 'describe spray: ', sp.stats.describe(spray_counter)
    print 'day std: ', sp.std(day_counter)
    print 'describe day: ', sp.stats.describe(day_counter)
    return
    
def prop():
    print bat_df.name.unique()

"""
Created on Wed Jun 22 14:37:48 2016

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

state = 'WA'
node_type = 'dcd'

last_report_death = pd.Timedelta('6 hours')
bat_thresh = 1.9

readprod = {
      "type": "postgresql",
      "data_source_provider": "rds",
      "adapter": "",
      "database": "prod",
      "encoding": "utf8",
      #"host": "prod.c2p0z5xffb9q.us-west-2.rds.amazonaws.com",
      "host": "prod-restored.c2p0z5xffb9q.us-west-2.rds.amazonaws.com",
      "username": "readonly",
      "password": "a90c1715e7866b8c1c61fb49cd7abd37",
      "reconnect": "true"
}

conn = fetch.get_psycopg2_connect(db_config = readprod)

        
query = (
        "SELECT AVG(l.battery) as battery_average, s.mac, "
        "p.name as prop, p.g_administrative_area_level_1 as state, MAX(l.stamp) as last_report "
        "FROM node.{} s "
        "LEFT JOIN LATERAL ( "        
            "SELECT * "
            "FROM log.{} log "
            "WHERE log.mac = s.mac "
            "ORDER BY log.stamp DESC "
            "LIMIT 6 ) l on TRUE "
        "JOIN properties p on p.id = s.property_id "
        "WHERE p.g_administrative_area_level_1 = '{}' "
        "GROUP BY s.mac, p.name, p.g_administrative_area_level_1 "
        "".format(node_type, node_type, state)
        )
         
bat_df = pd.read_sql(query, conn)


bat_df.battery_average = bat_df.battery_average.apply(lambda x: round(x, 1))
bat_df.loc[bat_df.battery_average <= bat_thresh, 'battery_average'] = 'Dead'
bat_df.loc[(bat_df.last_report < pd.Timestamp('now') - last_report_death) & (bat_df.battery_average <= bat_thresh), 
           'battery_average'] = 'Dead'
bat_df.loc[(bat_df.last_report < pd.Timestamp('now') - last_report_death) & (bat_df.battery_average > bat_thresh),
           'battery_average'] = 'Not Reporting'

volt_dummies = pd.concat([bat_df.prop, pd.get_dummies(bat_df['battery_average'], prefix = 'V')], axis = 1)
prop_df = volt_dummies.groupby('prop').sum()

prop_df = prop_df.apply(lambda x: x/sum(x), axis = 1)
prop_df = prop_df.applymap(lambda x: round(x, 2))
prop_df = prop_df.applymap(lambda x: x*100)
prop_df.to_csv('{} analysis in {}, as of {}.csv'.format(node_type, state, pd.Timestamp.today().date()))


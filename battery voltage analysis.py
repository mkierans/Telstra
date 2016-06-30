"""
Created on Mon Jun 20 10:17:29 2016

@author: matt_k
mot_amp = df.amp.loc[df.amp >= 0.5].mean()
df.amp.loc[df.amp <= 0.1] = 0
df.amp.loc[(df.amp >= 0.8) & (df.amp <= 1.2)] = mot_amp
df.amp.loc[df.amp > 1.2] = rad_amp + mot_amp 



"""


import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import datetime as dt

os.chdir('/home/matt/Semios/data')

def load_data_v2(file_name):
    df = pd.read_table(file_name, sep = ' ')
    df.columns = ['time', 'amp', 'units']
    df = df.loc[:, ['time', 'amp']]
    df.time = df.time.apply(lambda x: x.replace('_', ' ')) # replace '_' with ' ' so we can turn into date
    df.time = df.time.apply(lambda x: pd.to_datetime(x).time()) # turn str time into datetime object
    df.time = df.time.apply(lambda x: x.microsecond + 1e6*x.second + 60e6*x.minute)
    df.time = df.time.apply(lambda x: x/(1e6))
    return df

def load_data(file_name):
    df = pd.read_table(file_name, sep = ' ')
    df.columns = ['time', 'amp', 'units']
    df = df.loc[:, ['amp']]
#    df.time = df.time.apply(lambda x: pd.to_datetime(x).time()) # turn str time into datetime object
#    df.time = df.time.apply(lambda x: x.microsecond + x.second + 1e6*x.second + 60e6*x.minute)
#    df.time = df.time.apply(lambda x: x/(1e6))
    df = df.reset_index()
    df.columns = ['time', 'amp']
    df.time = df.time * 0.047987403 / 60
    return df

def load_data_numpy(file_name, samp_freq = 3.576879, skiprows = 7):
        
    assert skiprows >= 7, "Start of data file is not voltage, skiprows must be > 7"

    # Skip the first ~5 minute of weird data by default - can change skiprows
    amplitude = np.loadtxt(file_name, skiprows = skiprows)
    
    dt= 1/samp_freq # difference in time between samples
    time = np.arange(len(amplitude))*dt

    return pd.DataFrame({"time" : time, "amp" : amplitude})

def get_radio_times(df):
    
    low_thresh = 0.19 # lower bounds for radio voltage
    high_thresh = 0.22 # upper bounds for radio voltage
    rad_amp = df.amp.loc[(df.amp >= low_thresh) & (df.amp <= high_thresh)].mean() # finds average of radio voltage
    df.amp.loc[(df.amp > low_thresh) & (df.amp <= high_thresh)] = rad_amp # sets all radio voltages as average
        
    return df.loc[df.amp == rad_amp, 'time'].values # returns array of times where radio is on


def get_motor_times(df):
    
    low_thresh = 0.9
    high_thresh = 1.3
    return df.loc[(df.amp > low_thresh) & (df.amp < high_thresh), 'time'].values

def get_tmr(rad, mot):
    rad_diff_times = np.ediff1d(rad, to_end = 0)
    rad_times = rad[rad_diff_times > 1./60]
    
    mot_diff_times = np.ediff1d(mot, to_end = 0)
    mot_times = mot[mot_diff_times > 1./60]
    
    return rad_times, mot_times # this is to be worked on later

def plot_amp_time(df, low, high):
    fig = plt.figure(figsize = (12, 8))
    ax = fig.add_subplot(1,1,1)
    plt.scatter(df.time, df.amp)
    ax.axis([low/60, high/60, 0, 1.4]) # remember, every 600 is 10 minutes
    
def print_range(low, high, rad, mot):
    print "radio activations ", rad[(rad < high) & (rad > low)]
    print "motor activations ", mot[(mot < high) & (mot > low)]

inves_low = 0
inves_high = 3060


#lin_reg = []
#for count in range(9):
#   filename = '_test.csv'
#   df = load_data('0' + str(count) + filename)
#   lin_reg.append(sp.stats.linregress(df.time.values, df.amp.values))
   

df = load_data("15_idc_25min-sched_new_04-15-10.csv")
rad_times, mot_times = get_tmr(rad = get_radio_times(df), mot = get_motor_times(df))
plot_amp_time(df, inves_low, inves_high)
print_range(inves_low, inves_high, rad_times, mot_times)

# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 12:30:35 2020

@author: zhang
"""

import pandas as pd
import numpy as np
import glob
import re
import os
from matplotlib import pyplot as plt

def clean_data():
    all_data = pd.read_csv('all_data.csv')
    file_list = all_data['file name']
    tf_idf = pd.read_csv('result_tf_idf.csv')
    #process file name and search 
    file_list_clean = [fileid[36:] for fileid in file_list]
    tf_idf_file_list = tf_idf['file name']
    tf_idf_file_list = [fileid[29:] for fileid in tf_idf_file_list]
    
    tf_idf_file_list_clean = []
    for item in tf_idf_file_list:
        tf_idf_file_list_clean.append('2'+item.split("2", 1)[1])
    tf_idf['file name_clean'] = tf_idf_file_list_clean
    tf_idf= tf_idf.set_index('file name_clean')
    
    
    tf_idf_FIN = []
    tf_idf_H4N = []
    for i in range(len(file_list_clean)):
        try:
            tf_idf_FIN.append(tf_idf.loc[file_list_clean[i],'weighted-FIN-NEG'])
            tf_idf_H4N.append(tf_idf.loc[file_list_clean[i],'weighted-H4N-INF'])
        except:
            print(file_list_clean[i])
            tf_idf_FIN.append(None)
            tf_idf_H4N.append(None)
    
    all_data['weighted-FIN-NEG'] = tf_idf_FIN
    all_data['weighted-H4N-INF'] = tf_idf_H4N
    
    return all_data

def plot(all_data):
    #sort by Fin-neg
    all_data_FIN = all_data.sort_values(by = ['weighted-FIN-NEG'])
    median_list_FIN = []
    num_pergroup = int(all_data_FIN.shape[0]/5)
    for i in range(0, 4):
        df_group = all_data_FIN.iloc[i*num_pergroup:i*num_pergroup+num_pergroup]
        median_list_FIN.append(df_group['excess_return'].median())
        
    df_group_5 = all_data_FIN.iloc[4*num_pergroup:]
    median_list_FIN.append(df_group_5['excess_return'].median())
    
   
    all_data_H4N = all_data.sort_values(by = ['weighted-H4N-INF'])
    median_list_H4N = []
    num_pergroup = int(all_data_H4N.shape[0]/5)
    for i in range(0, 4):
        df_group = all_data_H4N.iloc[i*num_pergroup:i*num_pergroup+num_pergroup]
        median_list_H4N.append(df_group['excess_return'].median())
    df_group_5 = all_data_H4N.iloc[4*num_pergroup:]
    median_list_H4N.append(df_group_5['excess_return'].median())

    
    plt.plot(['low', 2, 3, 4,'high'], median_list_FIN, 'go-', label='Fin_Neg')
    plt.plot(['low', 2, 3, 4,'high'], median_list_H4N, 'bo-', label='Harvard_Neg')
    plt.title('Median Filling Period Return_tf_idf')
    plt.xlabel('Quintile(based on proportion of negative words)')
    plt.ylabel('Median Filing Period Excess Return(%)')
    plt.legend(loc='best')
    
    

if __name__ == '__main__':
    all_data = clean_data()
    plot(all_data)
    
    
    
    
    

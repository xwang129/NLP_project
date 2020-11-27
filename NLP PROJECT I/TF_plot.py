# -*- coding: utf-8 -*-
"""
This file is for plotting tHe graph of excess return based on term frequency 
of negative words. 
"""

import pandas as pd
from matplotlib import pyplot as plt

if __name__ == '__main__':
   
    all_data_FIN = pd.DataFrame()
    all_data_H4N = pd.DataFrame()
    #dictionary Fin neg
    for y in range(2010, 2019):
        # print(y)
        filename = '../DATA/stock_and_index/'+str(y)+'_dow30_excess_return.xlsx'
        df = pd.read_excel(filename)
        all_data_FIN = pd.concat([all_data_FIN,df]).sort_values(by=['% negative'])
        
    median_list_FIN = []
    num_pergroup = int(all_data_FIN.shape[0]/5)
    for i in range(0, 4):
        df_group = all_data_FIN.iloc[i*num_pergroup:i*num_pergroup+num_pergroup]
        #print(df_group['excess_return'])
        median_list_FIN.append(df_group['excess_return'].median())
    df_group_5 = all_data_FIN.iloc[4*num_pergroup:]
    median_list_FIN.append(df_group_5['excess_return'].median())
    
    #dictionary H4N
    for y in range(2010, 2019):
        # print(y)
        filename = '../DATA/stock_and_index_h/'+str(y)+'_dow30_excess_return.xlsx'
        df = pd.read_excel(filename)
        all_data_H4N = pd.concat([all_data_H4N,df]).sort_values(by=['% negative'])
        
    median_list_H4N = []
    num_pergroup = int(all_data_H4N.shape[0]/5)
    for i in range(0, 4):
        df_group = all_data_H4N.iloc[i*num_pergroup:i*num_pergroup+num_pergroup]
        #print(df_group['excess_return'])
        median_list_H4N.append(df_group['excess_return'].median())
    df_group_5_H4N = all_data_H4N.iloc[4*num_pergroup:]
    median_list_H4N.append(df_group_5_H4N['excess_return'].median())
    
    plt.plot(['low', 2, 3, 4,'high'], median_list_FIN, 'go-', label='Fin_Neg')
    plt.plot(['low', 2, 3, 4,'high'], median_list_H4N, 'bo-', label='Harvard_Neg')
    plt.title('Median Filling Period Return_tf')
    plt.xlabel('Quintile(based on proportion of negative words)')
    plt.ylabel('Median Filing Period Excess Return(%)')
    plt.legend(loc='best')
    


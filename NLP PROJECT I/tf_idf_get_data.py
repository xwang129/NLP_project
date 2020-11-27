# -*- coding: utf-8 -*-
"""
This file is for getting the tf-idf weight of each file
"""

import pandas as pd
import numpy as np
import glob
import re
import os

FILEPATH = '../Data/filtered_textdata'

def get_file_path():
    filepath_list =[]
    #iterate through the files, gather file name
   
    parent_dir = '../Data/stock_and_index_h/'
    for filepath in glob.iglob(os.path.join(parent_dir, '*.xlsx')): 
           filepath_list.append(filepath)
    return filepath_list[:-1]

def join_files(filepath_list):
    all_data = pd.DataFrame()
    for file in filepath_list:
        df = pd.read_excel(file)
        all_data = pd.concat([all_data,df])
        
    return all_data



if __name__ == '__main__':
   filepath_list = get_file_path()
   all_data = join_files(filepath_list)
   all_data.to_csv('all_data.csv')
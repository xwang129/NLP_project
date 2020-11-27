import pandas as pd
import numpy as np
import glob
import re


def iterate_year_files(year_filepath_list, year_dow30):
    # print(year_dow30)
    df = pd.DataFrame()
    for f in year_filepath_list: # interate quarter files
        print(f)
        df_f = pd.read_csv(f)
        # print(df_f.head())
        for index, row in df_f.iterrows():
            # print(row['company name'])
            file_name = row['file name']
            start = re.search("data_", file_name).span()
            end = file_name.find("_", start[1])
            cik = int(file_name[start[1]:end])
            row['cik'] = cik
            if cik in year_dow30 and '10-K' in row['file name'] and \
                    '10-K-A' not in row['file name']:  # get dow30 10k file
                df = df.append(row, ignore_index=True)
    # print(df.head())
    return df


def iterate_all_files(): 
    #get all file paths
    filepath_list = []
    for filepath in glob.iglob('../Data/Output-Stat_h/*.csv'): 
        # print(filepath)
        filepath_list.append(filepath)
    filepath_list.sort()
    # print(filepath_list)

    df_list = []
    # read in company label
    df_dow30_tic = pd.read_excel("../Data/stock_and_index/dow30_complete_cleaned.xlsx")[['cik', 'co_conm', 'co_tic']]
    # print(df_dow30_tic.head())
    # print(df_dow30_tic.head())
    df_Dow30 = pd.read_excel("../Data/stock_and_index/dow30_PerYear_cik.xlsx", index_col=0)
    # print(df_Dow30.head())
    for i in range(0, int(len(filepath_list)/4)): # interate over 4 files per year
        year_filepath_list = filepath_list[i*4:i*4+4]
        year_dow30 = list(df_Dow30.iloc[:,i])
        df_temp = iterate_year_files(year_filepath_list, year_dow30)
        df_temp = df_temp.merge(df_dow30_tic, left_on='cik', right_on='cik', how='left')
        # print(df_temp.columns)
        df_list.append(df_temp)

    # one dataframe for each year
    return df_list


def find_return(df_list):
    #The filing period return is the holding period excess return for the 10-k file date through the subsequent 3 days.
    #Excess return = firm's common stock buy-and-hold return - CRSP value-weighted market index buy-and-hold return
    
    #get CRSP index data
    df_CRSP_ind = pd.read_csv("../Data/stock_and_index/CRSP_value-weighted_index.csv")
    
    #get dow30 index data
    df_DJI_ind = pd.read_csv("../Data/stock_and_index/^DJI.csv")
    #integrate all years
    df_all_dow30_return = pd.DataFrame()
    
    df_Dow30 = pd.read_excel("../Data/stock_and_index/dow30_adj_price.xlsx")
    df_Dow30['DATE'] = pd.to_datetime(df_Dow30['DATE'], format='%m/%d/%y')
    df_Dow30 = df_Dow30.set_index('DATE')
    # print(df_Dow30.head())
    df_list_new = []
    year = 2010
    for df_PerYear in df_list: #every year
        df_temp = pd.DataFrame()
        df_PerYear['filed as of date'] = pd.to_datetime(df_PerYear['filed as of date'], format='%Y%m%d')
        for index, row in df_PerYear.iterrows(): #every stock
            ticker = row['co_tic'].upper()
            date = row['filed as of date']
            # find the filing date, calculate 4 day return
            adj_price_list = df_Dow30.loc[date:, ticker].head(4).tolist()
            CRSP_ind_list = df_CRSP_ind.loc[date.strftime("%Y%m%d"):, 'vwindx'].head(4).tolist()
            #TRY DJI
            DJI_ind_list = df_DJI_ind.loc[date.strftime("%Y/%m/%d"):,'Adj Close'].head(4).tolist() 

            #fourday_return = np.log(adj_price_list[3]) - np.log(adj_price_list[0])
            fourday_return = np.log(adj_price_list[3]/adj_price_list[0])
            CRSP_return = np.log(CRSP_ind_list[3]/CRSP_ind_list[0])
            DJI_return = np.log(DJI_ind_list[3]/DJI_ind_list[0])
            
            row['excess_return'] = fourday_return-CRSP_return
            
            df_temp = df_temp.append(row)
          
        df_list_new.append(df_temp)
        df_temp.to_excel('../Data/stock_and_index_h/'+str(year)+'_dow30_excess_return.xlsx')  
        year = year + 1
    return df_list_new


if __name__ == '__main__':
    print("find_Dow30.py")
    pd.set_option("display.max_columns", None)
    df_list = iterate_all_files() #dow30 stocks info without return
    print(len(df_list))
    df_list_all = find_return(df_list) #dow30 stocks info with return
    # print(df_list_all[0].head())
    # test_str = "G:/NLP-P1-Textdata/2017/QTR4/20171030_10-Q_edgar_data_37634_0000037634-17-000002_1.txt"
    #
    # start = re.search("data_", test_str).span()
    # end = test_str.find("_", start[1])
    # print(test_str[start[1]:end])

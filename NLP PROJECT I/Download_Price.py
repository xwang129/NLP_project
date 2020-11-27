from pandas_datareader import data
import pandas as pd

Dow30 = pd.read_excel("G:/NLP-P1-Textdata/dow30_complete.xlsx", sheet_name=0, index_col=0)
stocks = Dow30["co_tic"].values
data.DataReader(stocks, 'yahoo', "2010-01-01", "2020-11-22").to_csv("G:/NLP-P1-Textdata/dow30_ohlc.csv")





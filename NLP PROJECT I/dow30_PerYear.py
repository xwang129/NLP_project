import pandas as pd
import numpy as np
from datetime import date
from dateutil.rrule import rrule, YEARLY

if __name__ == '__main__':
	#read data into dataframe
	df_dow30_complete = pd.read_excel('dow30_complete_cleaned.xlsx', index_col=0)
	print(df_dow30_complete.head())

	# df_dow30_complete = df_dow30_complete.fillna('20200101')
	df_dow30_complete['from'] = pd.to_datetime(df_dow30_complete['from'], format='%Y%m%d')
	df_dow30_complete['thru'] = pd.to_datetime(df_dow30_complete['thru'], format='%Y%m%d')
	# print(df_dow30_complete.head())
	# print(df_dow30_complete.dtypes)

	df_dow30_PerYear = pd.DataFrame()
	# print(df_dow30_PerYear.head())
	#iterate from 2010 to 2019
	a = date(2010, 12, 31)
	b = date(2019, 12, 31)
	for dt in rrule(YEARLY, dtstart=a, until=b):
		dow30_ = []
		# print(dt.strftime("%Y%m%d"))
		for row in df_dow30_complete.iterrows():
			if row[1][1] < dt and row[1][2] > dt:
				dow30_.append(row[1]['cik'])
		# print(len(dow30_))
		df_dow30_PerYear[dt] = dow30_
	print(df_dow30_PerYear.head())
	df_dow30_PerYear.to_excel("../Data/stock_and_index/dow30_PerYear_cik.xlsx")

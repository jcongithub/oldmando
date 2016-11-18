import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import sys

from datetime import datetime
from datetime import timedelta
from printer import print_dataframe_as_table
import glob
from download import download_price_history

is_debug = False

if __name__ == '__main__':
	download_price_history('ashr')
	download_price_history('^ssec')

	pd.options.display.width = 1000
	ashr_df = pd.read_csv('data/ashr.price.csv')
	ashr_df = ashr_df.set_index('Date').sort_index(ascending=True)

	ssei_df = pd.read_csv('data/^ssec.price.csv')
	ssei_df = ssei_df.set_index('Date').sort_index(ascending=True)
	

	print('{} days price history from {} to {}'.format(len(ashr_df), ashr_df.iloc[len(ashr_df) - 1].name, ashr_df.iloc[0].name))
	print('{} days price history from {} to {}'.format(len(ssei_df), ssei_df.iloc[len(ssei_df) - 1].name, ssei_df.iloc[0].name))

	history = []
	for index, row in ashr_df.iterrows():
		try:
			date = index
			ashr = row['Close']
			ssei = ssei_df.ix[index]['Close']
			print("{} {} {}".format(index, ashr, ssei))

			history.append({'date' : date,
							'ashr' : ashr,
							'ssei' : ssei})
		except:
			print('')

	df = pd.DataFrame(history)

	df['diff'] = df['ssei'] - df['ashr']
	df['perc'] = (df['ashr']/df['ssei']) * 100

	print(df[['date', 'ssei', 'ashr', 'diff', 'perc']])

	df = df.set_index('date')
	df = df['2016-05-01':]
	print(df)
	print(df.describe())
	mean = df['perc'].mean()
	df['ssei_adj'] = df['ssei'] * 0.01 * mean

	df[['ashr', 'ssei_adj']].plot().grid(True)
	plt.show()
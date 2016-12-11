import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import sys

from datetime import datetime
from datetime import timedelta
import glob
from downloader import download_price_history

is_debug = False

if __name__ == '__main__':
	if(len(sys.argv) > 1):
		if(sys.argv[1] == '-d'):
			## download prices			
			download_price_history('ashr')
			download_price_history('^ssec')
		elif(sys.argv[1] == '-h'):
			print('show ashr and ^ssec history price')
			print('ashr [-d]')
			print('\t -d download history price data')
			sys.exit()

	pd.options.display.width = 1000
	ashr_df = pd.read_csv('data/ashr.price.csv')
	ashr_df = ashr_df.set_index('Date').sort_index(ascending=True)

	ssei_df = pd.read_csv('data/^ssec.price.csv')
	ssei_df = ssei_df.set_index('Date').sort_index(ascending=True)
	

	print('{} days price history from {} to {}'.format(len(ashr_df), ashr_df.iloc[len(ashr_df) - 1].name, ashr_df.iloc[0].name))
	print('{} days price history from {} to {}'.format(len(ssei_df), ssei_df.iloc[len(ssei_df) - 1].name, ssei_df.iloc[0].name))

	## merge 
	history = []
	for index, row in ashr_df.iterrows():
		try:
			date = index
			ashr = row['Close']
			ssei = ssei_df.ix[index]['Close']

			history.append({'date' : date,
							'ashr' : ashr,
							'ssei' : ssei})
		except:
			print('')

	#calculte 'diff' and 'percentage'
	df = pd.DataFrame(history)
	df['diff'] = df['ssei'] - df['ashr']
	df['perc'] = (df['ashr']/df['ssei']) * 100
	df = df.set_index('date')
	

	#Draw chart
	df = df['2016-01-01':][['ashr', 'ssei', 'perc']]
	ashr_mean = df['ashr'].mean()
	ssei_mean = df['ssei'].mean()
	perc_mean = df['perc'].mean()
	print(df)
	print('ashr mean price:{} sseu mean price:{} perc mean:{}'.format(ashr_mean, ssei_mean, perc_mean))
	print(df.describe())
	df['ashr'] = df['ashr'] / ashr_mean
	df['ssei'] = df['ssei'] / ssei_mean

	df.plot().grid(True)
	plt.show()
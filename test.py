import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import sys

d = [
	{'name':'A', 'money': 0, 'point': 100}, 	
	{'name':'B', 'money': 1, 'point' :200}, 	
	{'name':'A', 'money': -1, 'point':300}, 	
	{'name':'A', 'money': -1, 'point':400}	
]


df = pd.DataFrame(d)

print(df)

grouped = df.groupby('name') 
print(grouped.agg(['count', 'sum', 'mean']))

print(grouped.agg({'money' : sum, 'point' :max}))

print(grouped.agg({'money' : sum, 'point' :max}).join(grouped.agg({'money' :max})))

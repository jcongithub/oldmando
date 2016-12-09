import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import sys

def info(object, spacing=10, collapse=1):
	"""Print methods and doc strings.
    Takes module, class, list, dictionary, or string."""
	methodList = [method for method in dir(object) if callable(getattr(object, method))]
	processFunc = collapse and (lambda s: " ".join(s.split())) or (lambda s: s)
	print ("\n".join(["%s %s" % (method.ljust(spacing), processFunc(str(getattr(object, method).__doc__))) for method in methodList]))


df = pd.DataFrame({'animal': 'cat dog cat fish dog cat cat'.split(), 'size': list('SSMMMQL'), 'weight': [8, 10, 11, 1, 20, 13, 12], 'adult' : [False] * 5 + [True] * 2})

print(df)

print(df.sort_values(['animal', 'weight']))


idx = df.groupby('animal')['weight'].idxmax()

print(df.loc[idx, ['animal', 'size', 'weight']].set_index('animal'))

#print(df.groupby('animal').apply(lambda x : len(x[x.weight > 11])))

mhc = {'count' : lambda x : len(x[x.weight > 11]), 'max' : np.max}
#print(type(grouped))
#print(info(grouped))

print('df type:{}'.format(type(df)))

grouped = df.groupby('animal')
print('grouped type:{}'.format(type(grouped)))
print(grouped.groups)
print(grouped.groups['cat'])
print(grouped.indices['cat'])
print("get_groups['cat']:\n{}".format(grouped.get_group('cat')))

#max_weight_animal = df.groupby('animal').apply(lambda subf: subf['size'][subf['weight'].idxmax()])
#max_weight_animal = df.groupby('animal').apply(lambda subf: subf.loc[subf['weight'].idxmax()])

print(df[df.weight > 10])
print('-----')

def f(group):
	data = pd.DataFrame({'maxweight' : [group['weight'].idxmax()], 
						 'numbig'    : [len(group[group.weight > 10])]})
	return data

max_weight_animal = df.groupby('animal').apply(f)
print(max_weight_animal)
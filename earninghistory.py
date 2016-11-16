from bs4 import BeautifulSoup
from urllib.request import urlopen
#src = 'http://www.streetinsider.com/ec_earnings.php?q=GOOG'
src = "http://www.streetinsider.com"
html = urlopen(src).read().decode('utf-8')
print(html)
print("--------------------------")

soup = BeautifulSoup(html, 'html.parser')

tables = soup.find_all('table')

for table in tables:
	trs = table.find_all('tr')
	for tr in trs:
		tds = tr.find_all('td')
		for td in tds:
			print(td.get_text(), end=" ", flush=True)
			
		print('')



class EarningReport(object):
	def __init__(self,  date, qtr, eps, eps_cons, surprise, revs, revs_cons):
		self.date = date
		self.qtr = qtr
		self.eps = eps
		self.eps_cons = eps_cons
		self.surprise = surprise
		self.revs = revs
		self.revs_cons = revs_cons


#for tr in trs:
	#print(tr)
	#tds = tr.find_all('td')
	#for td in tds:
	#	print(td.get_text())	


import requests
from requests_cache.backends.mongodb import MongoCache
from requests_cache.session import CachedSession
from bs4 import BeautifulSoup
from urllib.error import HTTPError
import pandas as pd
import datetime
import pymongo 

class MongoSectorPerformance:

	def __init__(self):
		url = 'https://eresearch.fidelity.com/eresearch/goto/markets_sectors/landing.jhtml'
		#backend = MongoCache(connection=clientInstance)
		creds = open(os.path.dirname(__file__)+'/../credentials.txt')
		conn_str = str(creds.read())
		clientInstance = pymongo.MongoClient(conn_str)
			with requests.Session() as s:
				page = s.get(url, timeout=5).text

		except HTTPError as error:
			print(error.code)
			
		soup = BeautifulSoup(page, 'html.parser')
		
		sectors = soup.find_all('a', {'class': 'heading1'})
		pct_change = soup.find_all('td', {'class': 'change_td'})

		#adjust for your database/collection names
		mdb = clientInstance['marketdb'] 
		sector_col = mdb['sector_performance']
		sector_col.insert_one(self.format_dict(_keys=sectors, _vals=pct_change))

	def format_dict(self, _keys, _vals):
		d = {}
		for tag in _keys:
			#print(tag.string)
			d[tag.string] = None

		for idx, tag in enumerate(_vals):
			keyIndex = list(d)[idx]
			d.update({str(keyIndex): tag.string})

		date_id = datetime.datetime.now()
		return {str(date_id):d}

if __name__ == '__main__':
	MongoSectorPerformance()
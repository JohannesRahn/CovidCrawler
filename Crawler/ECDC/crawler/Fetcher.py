import requests
import mysql.connector as mariadb
import datetime
import json
from datetime import datetime
import copy


class Fetcher:
	
	def fetch(self):
		url = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/json/'
		req = requests.get(url)
		output = json.loads(req.content)
		return output
			
	def printDB(self, toPrint):
		toCheck = copy.deepcopy(toPrint)
		counter = 0
		mariadb_connection = mariadb.connect(host='XXXXX',user='XXXXX', port='XXXXX', password='XXXXX', database='AdvancedProgramming')
		cursor = mariadb_connection.cursor()
		# List is used for checking whether the element in the JSON Object needs to be converted to an integer.
		toIntConv = ['day','month','year','cases','deaths','popData2019']
		
		cursor.execute("DELETE FROM ECDC_Data")
		for elem in toPrint['records']:
			print(str(counter) + '/' + str(len(toPrint['records'])))
			# Calculates the whole cases
			totalNumbers = self.sumTimeSeries(elem, toCheck)
			
			# Sometimes the ECDC messes up the country codes into an unknown code. Empty string will be passed to avoid conflicts and wrong data.
			
			if elem['geoId'] is not None:
				if len(elem['geoId']) >2:
					elem['geoId'] = ""
			else: 
				elem['geoId'] = ""
			if elem['countryterritoryCode'] is not None:
				if len(elem['countryterritoryCode']) >3:
					elem['countryterritoryCode'] = ""
			else: 
				elem['countryterritoryCode'] = ""
			for key in elem:
				# Used for parsing the values into the right format to pass into Database.
				if key in toIntConv and not isinstance(elem[key], int):
					if elem[key] is not None:
						elem[key] = elem[key].strip()
			
						if elem[key].isnumeric():
							elem[key] = int(elem[key])
						elif elem[key] == "":
							# Int gets a negative value if empty, as the Mysql.connector does not properly convert a Python 'None' to a SQL 'NULL' value (at least when the None Value is supposed to be an SQL
							# Integer). 
							# This will not become an issue as negative values are not possible in that specific data set (There are no deltas calculated only absolute numbers).
							# The data will automatically be set to 'NULL' by an SQL Trigger. 
							elem[key] = -1
					else: 
						elem[key] = 0
						
			try:
				cursor.execute("INSERT INTO ECDC_Data VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')"
				.format(elem['dateRep'], elem['day'], elem['month'], elem['year'], totalNumbers[0], totalNumbers[1], elem['cases'], elem['deaths'], elem['countriesAndTerritories'].replace('_',' '), elem['geoId'], elem['countryterritoryCode'], elem['popData2019'], elem['continentExp']))
				
			except Exception as e:
				# No rollback as it only affects one data set which will be ignored for todays data. 
				print(elem['countriesAndTerritories'])
				print("Fehler" + str(e))
			counter+=1
		mariadb_connection.commit()
		mariadb_connection.close()
		
		
	def sumTimeSeries(self, dataSetCountry, toPrint):
		# Had to use the brute force method as Dictionaries do not have to be ordered.
		# Not the best solution in terms of runtime. I did not have a lot time to implement this and will try to get a more elegant solution if I have time.
		
		country = dataSetCountry['countriesAndTerritories']
		
		toCompareDate = datetime.strptime(dataSetCountry['dateRep'], '%d/%m/%Y')
		totalCases=0
		totalDeaths= 0
		
		for elem in toPrint['records']:
			
			if elem['countriesAndTerritories'] == country:
				toBeCompared = datetime.strptime(elem['dateRep'], '%d/%m/%Y')
				if toCompareDate >= toBeCompared:
					totalCases += int(elem['cases'])
					totalDeaths += int(elem['deaths'])
		return totalCases, totalDeaths
				

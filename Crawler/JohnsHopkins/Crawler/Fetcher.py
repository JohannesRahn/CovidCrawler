import requests
import csv
import mysql.connector as mariadb

# 1. All Csv files get downloaded from the Johns Hopkins Repository. 
# 2. Compare the headers of all three csv file in order to make sure, that all csv files are on the same version. Also the length of the documents will be compared.
# 3. The contents of files (skipping the header row) are stored in three different dictionaries where the key for each dictionary is the name of the country and the value is the rest of the CSV's row as a list
#	 For some countries the information is stored on a state/province level. As our project only intents to compare countries (except for Germany), the cases/deaths/recovered numbers of the states will be
#    summed up into one dictionary using [a[i]+b[i] for i in range(len(a))]
# 4. The entire data of the database will be deleted and all the downloaded date will be passed into the database in order to get the latest information (even on events in the past).
# INFO: Some countries are called different in the ECDC data set, (e.g. South Korea -> Korea, South). I take care of these error in a SQL Trigger but also an implementation in Python is suitable.
class Fetcher():

	def fetch(self, link):
		countryDictionary = {}
				
		with requests.Session() as s:
			download= s.get(link)
			decoded_content = download.content.decode('utf-8')
			cr = csv.reader(decoded_content.splitlines(), delimiter=',')
			content_list = list(cr)
		for row in content_list:
			#Checks if the Key already exists in the dictionary
			if row[1] in countryDictionary:
				# Calls method to sum up each value in the two lists and sets the resulting list as the new value for the existing key
				countryDictionary[row[1]] = self.sumLines(countryDictionary[row[1]], row[4:])
				
			else:
				# converts the values into integers if it is not the header row
				countryValue = []
				if row[1] == 'Country/Region':
					countryValue = row[4:]
					countryDictionary[row[1]] = countryValue
				else:
					countryValue = [int(row[4:][i]) for i in range(len(row[4:]))]
					countryDictionary[row[1]] = countryValue
		return countryDictionary
			
	def sumLines(self, dictionary , toAdd):
		# Only works if lists have the same length. This should always be the case in a csv file though.
		retSum = []
		for i in range(len(dictionary)):
			retSum.append(int(dictionary[i])+ int(toAdd[i]))
		return retSum
		
	def printDB(self, header, cases , deaths, recovered):
		mariadb_connection = mariadb.connect(host='XXXXX',user='XXXXX', port='XXXXX', password='XXXXX', database='AdvancedProgramming')
		cursor = mariadb_connection.cursor()
		newCases = 0
		newDeaths = 0
		# Delete existing Data in Database in order to have always the most up-to-date data.
		cursor.execute("DELETE FROM Hopkins_Data")
		for key in cases:
			day = ''
			month = ''
			year = ''
						
			for i in range(len(cases[key])):
				
				splitDateRep = header[i].split('/')
				# Calculating the new cases for every day
				if i != 0:
					newCases = cases[key][i] - cases[key][i-1]
					newDeaths = deaths[key][i] - deaths[key][i-1]
				else:
					newCases = cases[key][i]
					newDeaths = deaths[key][i]
				month = splitDateRep[0]
				day = splitDateRep[1]
				year = splitDateRep[2]
				try:
					cursor.execute("INSERT INTO Hopkins_Data VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')"
					.format(header[i], day, month, year, key.replace("\'", "\\'").replace('_', ' '), newCases, newDeaths,  cases[key][i], deaths[key][i], recovered[key][i]))
				
				except Exception as e:
					print("Error: " + str(e))
					mariadb_connection.rollback()
		mariadb_connection.commit()
		mariadb_connection.close()

import requests
import csv
import mysql.connector as mariadb

# 1. CSV File gets downloaded from https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv?cachebust=d58b419300b3a884
# 2.. For some countries the information is stored on a state/province level. As our project only intents to compare countries (except for Germany) we only collect data from the relevant sources.
# 3. The entire data of the database will be deleted and all the downloaded date will be passed into the database in order to get the latest information (even on events in the past).
# INFO: Some countries are called different in the ECDC data set, (e.g. South Korea -> Korea, South). I take care of these error in a SQL Trigger but also an implementation in Python is suitable.
class Fetcher():

	def fetch(self, link):
				
		with requests.Session() as s:
			countryList = []
			download= s.get(link)
			decoded_content = download.content.decode('utf-8')
			cr = csv.reader(decoded_content.splitlines(), delimiter=',')
			content_list = list(cr)
		for row in content_list:
			if row[0] == 'country_region_code':
				continue
			
			# As we only intent to compare on a country level (except for RKI) this condition will skip every row where a state is inserted except for Germany.
			if (row[2] == '' and row[3] == '') or row[0] == 'DE':
				countryList.append(row)
				
		return countryList
		
	def printDB(self, mobilityData):
		mariadb_connection = mariadb.connect(host='XXXXX',user='XXXXX', port='XXXXX', password='XXXXX', database='AdvancedProgramming')
		cursor = mariadb_connection.cursor()
		cursor.execute("DELETE FROM mobility_Data")
		counter = 0
		for elem in mobilityData:
			date = elem[6].split('-')
			day = int(date[2])
			month = int(date[1])
			year = int(date[0])
			if len(elem) < 12:
				for i in range(len(elem), 12):
					elem.append(0)
			if '' in elem:
				for i in range(7, len(elem)):
					if elem[i] == '':
						elem[i] = 0
				
			if elem[0] != 'DE' or (elem[0] == 'DE' and elem[2] == '' and elem[2] == ''):
				cursor.execute("INSERT INTO mobility_Data (ID, countryCode, dateRep, country, day, month, year, retailAndRecreation, groceryAndPharmacy,  parks, transit, workplaces, residential) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(counter, elem[0], elem[6], elem[1].replace('\'','').replace("\'", "\\'").replace('_', ' '), day, month, year, int(elem[7]), int(elem[8]), int(elem[9]), int(elem[10]), int(elem[11]), int(elem[12])))
			else:
				cursor.execute("INSERT INTO mobility_Data (ID, countryCode, dateRep, country, state, day, month, year, retailAndRecreation, groceryAndPharmacy,  parks, transit, workplaces, residential) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(counter, elem[0], elem[6], elem[1], elem[2] ,day, month, year, int(elem[7]), int(elem[8]), int(elem[9]), int(elem[10]), int(elem[11]), int(elem[12])))
			counter += 1
				
		mariadb_connection.commit()
		mariadb_connection.close()

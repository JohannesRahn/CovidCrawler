import requests
from bs4 import BeautifulSoup
import csv
import mysql.connector as mariadb
from crawler import CrawledBundesland 
import datetime


class Fetcher():

	def fetch(self):
		r = requests.get("https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Fallzahlen.html")
		doc = BeautifulSoup(r.text, "html.parser")
		inhalt = []
		retBundeslaender = []

		# Loops through all list elements.
		for tr in doc.select("tr"):            
			if tr.select("td"):
				inhalt = []
				# Loops through all contents of thel ist element and cretes an object.
				for td in tr.select("td"):
					
					inhalt.append(td.text.replace('.', '').replace('*','').replace(',', '.').replace('*', '').replace('­', '').replace("\n","").replace("&shy;","").replace('\"','').strip())

				# Some manual corrections because of changing hyphens on the RKI website.
				bundeslandName = str(inhalt[0]).replace('"', '')
				if(bundeslandName.startswith("Meck")):
					bundeslandName = "Mecklenburg-Vorpommern"
				elif(bundeslandName.startswith("Nieder")):
					bundeslandName = "Niedersachsen"

				bundeslaender = CrawledBundesland(bundeslandName, int(inhalt[1]), int(inhalt[2]),int(inhalt[5]))
				retBundeslaender.append(bundeslaender)
		return retBundeslaender

	def printDB(self, toPrint):
		today= datetime.datetime.today()
		tag = today.day
		monat = today.month
		jahr = today.year

		mariadb_connection = mariadb.connect(host='XXXXX',user='XXXXX', port='XXXXX', password='XXXXX', database='AdvancedProgramming')
		cursor = mariadb_connection.cursor()
		# Passes data into database
		for row in toPrint:
			cursor.execute("INSERT INTO RKI_Data (day , month, year, bundesland, totalCases, cases, deaths) VALUES ('{}','{}','{}','{}','{}','{}','{}')".format(tag, monat ,jahr, 
			row.bundesland, row.anzahl,row.differenz, row.todesfaelle))
		mariadb_connection.commit()

	def printBackupFile(self):
		# In case anything goes wrong the html file is saved as text. 
		
		r = requests.get("https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Fallzahlen.html")
		
		todayDate = datetime.datetime.today().strftime("%Y-%m-%d")
		backupFile = '../../Backup_Data/RKI_Crawler_Except/RKI' + todayDate + '.txt'
		with open(backupFile, 'a+') as txtFile:
			txtFile.write(todayDate +'\n')
			txtFile.write(r.text)

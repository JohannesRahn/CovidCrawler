# printDB(self, header, cases , deaths, recovered)
# 1. All Csv files get downloaded from the Johns Hopkins Repository. 
# 2. Compare the headers of all three csv file in order to make sure, that all csv files are on the same version. Also the length of the documents will be compared.
# 3. The contents of files (skipping the header row) are stored in three different dictionaries where the key for each dictionary is the name of the country and the value is the rest of the CSV's row as a list
#	 For some countries the information is stored on a state/province level. As our project only intents to compare countries, the cases/deaths/recovered numbers of the states will be
#    summed up into one dictionary using [a[i]+b[i] for i in range(len(a))]
# 4. The entire data of the database will be deleted and all the downloaded date will be passed into the database in order to get the latest information (even on events in the past).
from time import sleep
import Crawler

CASES_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
DEATHS_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
RECOVERED_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'
sameHeader = True
print("Data is being fetched...")
while sameHeader:
	try:
		fetcher = Crawler.Fetcher()
		cases = fetcher.fetch(CASES_URL)
		deaths = fetcher.fetch(DEATHS_URL)
		recovered = fetcher.fetch(RECOVERED_URL)
		header = []
	except:
		# Not as critical, as also historical data is online.
		print('Fehler')
		

	header.append(cases.pop('Country/Region'))
	header.append(deaths.pop('Country/Region'))
	header.append(recovered.pop('Country/Region'))
	#Checks whether the headers and rows of the csv files are the same in order to see wheter all files are on the same version. 
	if header[0] == header[1] and header[0] == header[2] and len(cases) == len(deaths) and len(cases) == len(recovered):
		print('Data gets passed into Database...')
		fetcher.printDB(header[0], cases,deaths,recovered)
		sameHeader = False
		
	# If not program will sleep for one hour and try again.
	else:
		print("Headers are not the same. Next try is in one hour.")
		sleep(3600)

print("Done")

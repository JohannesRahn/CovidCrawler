import Crawler

Mobility_URL = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv?cachebust=d58b419300b3a884'

print("Data is being fetched...")


fetcher = Crawler.Fetcher()
mobilityData = fetcher.fetch(Mobility_URL)
print('Data gets passed into Database...')
fetcher.printDB(mobilityData)

print("Done")

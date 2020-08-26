import crawler

fetcher = crawler.Fetcher()
try:
	toPrint = fetcher.fetch()
	fetcher.printDB(toPrint)
except:
	fetcher.printBackupFile()

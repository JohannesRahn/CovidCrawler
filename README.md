Repository for crawling Covid-19 data from different major data sources.
In the backup folder you can find backups from the database. This is only relevant for the RKI as the data from this source is crawled from a website where no historic data is displayed. 

The sources of data are:

worldwide:

- ECDC
- Johns Hopkins University
- Google Mobility data


Germany:
- RKI (incl. state level on a daily basis)

The Backup_Data folder provides all SQL dumps of the database until the beginning of July. After that the database got migrated to a Google Cloud SQL Database.

Important note for usage: 
- there  are a few triggers set on the database. Not all of them are in the SQL Trigger file. You might want to load the Backup file "Cloud_SQL_Export_2020-08-25 (08_13_45)" in order to have everything set up the way it is supposed to be.

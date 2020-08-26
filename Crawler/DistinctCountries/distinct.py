# As a join function was too complex for the Database and took too long this is used for country comparrison. 

import mysql.connector as mariadb

mariadb_connection = mariadb.connect(host='XXXXX',user='XXXXX', port='XXXXX',password='XXXXX', database='XXXXX')
cursor = mariadb_connection.cursor()
cursor.execute("DELETE FROM distinctCountries")
cursor.execute("SELECT DISTINCT countriesAndTerritories FROM AdvancedProgramming.ECDC_Data")
distinctCountriesECDC = cursor.fetchall()
cursor.execute("SELECT DISTINCT country FROM AdvancedProgramming.Hopkins_Data")
distinctCountriesHopkins = cursor.fetchall()
cursor.execute("SELECT DISTINCT country FROM AdvancedProgramming.mobility_Data")
distinctCountriesMobility = cursor.fetchall()
distinctCountries= list(set(set(distinctCountriesECDC).intersection(distinctCountriesHopkins).intersection(distinctCountriesMobility)))

for row in distinctCountries:
	cursor.execute("INSERT INTO distinctCountries VALUES ('{}')".format(row[0]))
mariadb_connection.commit()

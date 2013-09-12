'''
Created on Aug 9, 2013

@author: Scott.Embler
'''
import sys
import os
import crnlite.database as db

if __name__ == '__main__':
	db_file = sys.argv[1]

	if not os.path.exists(db_file):
		print('Initializing ' + db_file)
		db.define_schema(db_file)

	print('Synchronizing station data to ' + db_file)
	db.sync_station_metadata(db_file)

	print('Synchronizing Hourly02 data to ' + db_file)
	db.sync_hourly02(db_file)

	print('Synchronizing Daily01 data to ' + db_file)
	db.sync_daily01(db_file)

	print('Synchronizing Monthly01 data to ' + db_file)
	db.sync_monthly01(db_file)

	print('Synchronizing Subhourly01 data to ' + db_file)
	db.sync_subhourly01(db_file)

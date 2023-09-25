import datetime
import requests
from dotenv import load_dotenv, dotenv_values
from Conn_Common import *

conn_api()
try:
    conn_db()
    print("DataBase connected!")
except Exception:
    print("Expection")

cursor = conn_db().cursor()
# read txt file into database table
url1 = "http://10.222.10.17/BI/dataout/publisher/egm/egm.txt"
response = requests.get(url1)
try:
 cursor.execute("truncate bi_analysis_adhoc.nsw_barooga_egm")
 print("Egm Data Deleted!")
except Exception as err:
	print("err")

lines = response.text.splitlines()
# remove the header line
headers = lines[0]
lines = lines[1:]
# insert into Egms talbe
try:
	inser_query = '''
			insert into bi_analysis_adhoc.nsw_barooga_egm (
				EgmPK,
				fpn,
				SerialNo,
				GMID,
				Manufacturer,
				InstallDate,
				DelisensedDate,
				EgmType,
				denom,
				ChangeDate,
				venuename,
				venuecode
			)
			values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'SPORTIES BAROOGA', 'TN7020')	
	'''
	# Insert each record into the table
	for line in lines:
		values = line.split(',')  # Split by comma
		if 'ST' in values[1]:
			continue
		# Convert InstallDate to string
		values[5] = datetime.datetime.strptime(values[5], '%Y-%m-%d') \
			.date().strftime('%Y-%m-%d') if values[5] != '' else None
		values[6] = datetime.datetime.strptime(values[6], '%Y-%m-%d') \
			.date().strftime('%Y-%m-%d') if values[6] != '' else None
		values[9] = datetime.datetime.strptime(values[9], '%Y-%m-%d %H:%M:%S') \
			.date().strftime('%Y-%m-%d %H:%M:%S') if values[9] != '' else None
		cursor.execute(inser_query, values)
	conn_db().commit()
	print('Egms import Success!')
	conn_db().close()
	cursor.close()
except Exception as error:
	print(error)
from Conn_Common import *
from lxml import etree

conn_api()
info_conn_db()

cursor = conn_db().cursor()

url = 'http://10.1.1.117/BI/dataout/publisher/Demo/'
res = requests.get(url)
response = res.text
res_etree = etree.HTML(response)
# Parsing file folder path
res_link = res_etree.xpath('//tr//td//@href')

# Trancate table of Egmactivity
try:
	cursor.execute("truncate aaa.bbb_ccc")
	print("Demo Data Deleted!")
except Exception as err:
	print("err")

#print(df_active)

#
for filename in res_link[:]:  # Create a copy of the list to iterate over
    if 'txt' not in filename: # filter out other folder and txt without data
        res_link.remove(filename)
    else:
        url_x = url + filename
        res_file = requests.get(url_x)
        print(url_x)
        if len(res_file.content) == 0:
            print(filename + " is Empty")
            continue
        else:
            lines = res_file.text.splitlines()  # Use res_file.text instead of response
            #print(check_header(url_x))
            n=1
            value = 'Demo_Date'
            if bool(check_header1(url_x, n, value)):
                headers = lines[0]
                lines = lines[1:]
            else:
                lines = lines
            try:
                inser_query = '''
                        insert into aaa.bbb_ccc (
                            Demo_Date,
                            AAAPK,
                            BBBPK,
                            Serno,
                            Value1,
                            Value2,
                            Value3,
                            Value4,
                            Value5,
                            Value6,
                            Value7,
                            Value8,
                            Value9,
                            Value10,
                            Value11,
                            Value12,
                            Value13,
                            Value14,
                            maaa
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '1')    
                '''
                # Insert each record into the table
                for line in lines:
                    values = line.split(',')  # Split by comma
                    if 'AA' in values[3]:
                        continue
                    # Convert InstallDate to string
                    values[0] = datetime.datetime.strptime(values[0], '%Y-%m-%d') \
                        .date().strftime('%Y-%m-%d') if values[0] != '' else None
                    values[5] = float(values[5]) / 100
                    values[7] = float(values[7]) / 100
                    values[8] = float(values[8]) / 100
                    values[9] = float(values[9]) / 100
                    values[10] = float(values[10]) / 100
                    values[11] = float(values[11]) / 100
                    values[12] = float(values[12]) / 100
                    values[13] = float(values[13]) / 100
                    values[15] = float(values[15]) / 100
                    values[16] = float(values[16]) / 100
                    values.append(values[5] - values[7])
                    # print(values)
                    cursor.execute(inser_query, values)
                conn_db().commit()
                print(filename + ' import Success!')
            except Exception as error:
                print(error)

# Close the cursor and connection after the loop finishes
cursor.close()
conn_db().close()

print("Demo Data import Success!")

		#response = requests.get(url_x)







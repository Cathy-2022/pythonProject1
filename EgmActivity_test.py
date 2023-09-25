import datetime
import requests
from Conn_Common import *
from lxml import etree

conn_api()
info_conn_db()

cursor = conn_db().cursor()

url = 'http://10.222.10.17/BI/dataout/publisher/egmActivity/'
res = requests.get(url)
response = res.text
res_etree = etree.HTML(response)
res_link = res_etree.xpath('//tr//td//@href')
info_conn_db()

#try:
    #cursor.execute("truncate bi_analysis_adhoc.nsw_barooga_egmactivity")
    #print("Egmactivity Data Deleted!")
#except Exception as err:
    #print("err")

def check_header(file_url):
    res_file = requests.get(file_url)
    lines = res_file.text.splitlines()
    if len(lines) > 0:
        return lines[0].startswith('GamingDate')
    return False

for filename in res_link[:]:  # Create a copy of the list to iterate over
    if '2021' not in filename: # filter out other folder and txt without data
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
            if bool(check_header(url_x)):
                headers = lines[0]
                lines = lines[1:]
            else:
                lines = lines
            try:
                inser_query = '''
                        insert into bi_analysis_adhoc.nsw_Barooga_egmactivity (
                            GamingDate,
                            EgmPK,
                            GameVariationPK,
                            fpn,
                            PJKP_pct,
                            Turnover,
                            Stroke,
                            CreditWin,
                            PjkpWins,
                            CancelCredit,
                            CashlessIn,
                            CashlessOut,
                            CashIn,
                            CashOut,
                            CoinOut,
                            CardToEgm,
                            PtsToEgm,
                            Revenue,
                            egmdays
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '1')    
                '''
                # Insert each record into the table
                for line in lines:
                    values = line.split(',')  # Split by comma
                    if 'ST' in values[3]:
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


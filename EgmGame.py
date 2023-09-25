import re
from Conn_Common import *

conn_api()

try:
    conn_db()
    print("Database connected!")
except Exception as err:
    print("Exception")

cur = conn_db().cursor()
url = "http://10.222.10.17/BI/dataout/publisher/egmGames/EgmGameVariation.txt"
response = requests.get(url)

try:
    cur.execute("truncate bi_analysis_adhoc.nsw_Barooga_egmgame")
    print("Egmgame Data Deleted!")
except Exception as err:
    print("Error:", str(err))

lines = response.text.splitlines()
headers = lines[0]
lines = lines[1:]

try:
    insert_query = '''
        insert into bi_analysis_adhoc.nsw_Barooga_egmgame(
            GameVariationPK,
            GameName,
            Manufacturer,
            Variation,
            Denom,
            RTP,
            MaxBet,
            ChangeDate
        )
        values(%s, %s, %s, %s, %s, %s, %s, %s)
    '''

    for line in lines:
        values = re.split(r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)',line)
        values = [v.replace('"','') for v in values]

        values[-1] = datetime.datetime.strptime(values[-1], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

        cur.execute(insert_query, values)

    conn_db().commit()
    print('EgmGames import success!')
    conn_db().close()
    cur.close()
except Exception as err:
    print(str(err))
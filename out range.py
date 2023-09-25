from Conn_Common import *
from lxml import etree

conn_api()
url = 'http://10.222.10.17/BI/dataout/publisher/egmActivity/'
res = requests.get(url)
response = res.text
res_etree = etree.HTML(response)
res_link = res_etree.xpath('//tr//td//@href')

for filename in res_link[:]:  # Create a copy of the list to iterate over
    if 'txt' not in filename: # filter out other folder and txt without data
        res_link.remove(filename)
    elif '09:55' in res_link[:]:
        url_x = url + filename
        res_file = requests.get(url_x)
        print(url_x)

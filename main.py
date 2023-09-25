from Conn_Common import *
from lxml import etree


url = 'http://10.222.10.17/BI/dataout/publisher/egmActivity/'
res = requests.get(url)
response = res.text
res_etree = etree.HTML(response)
res_link = res_etree.xpath('//tr//td//@href')

for filename in res_link[:]:
    url_x = url + filename
    res_file = requests.get(url_x)
    #print(url_x)
    if len(res_file.content) == 0:
        print(filename + " is Empty")
        continue
    else:
        lines = res_file.text.splitlines()
        for line in lines:
            values = line.split(',')  # Split by comma
            if values[0]=='2022-09-21' and values[3]=='0504':
                print(url_x)
            else:
                continue
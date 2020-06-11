from modbus.client import *
import csv, time, datetime, telebot
from telebot import apihelper

order = ['Дата Время', 'ГПГУ 1 ', 'ГПГУ 2 ', 'ГПГУ 3 ', 'ГПГУ 4 ', 'ГПГУ 5 ']

apihelper.proxy = {'https':'socks5://cx1b2j:E1caTT@186.65.117.60:9396'}
token = '827576612:AAEX0IHqMW5x-oWrh8T1ZXhE-9_K8pXMTJ0'
bot = telebot.TeleBot(token)

data_file = "data.csv"
with open(data_file, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['Дата Время', 'ГПГУ 1 ', 'ГПГУ 2 ', 'ГПГУ 3 ', 'ГПГУ 4 ', 'ГПГУ 5 '])
    

c = client(host="192.168.127.254", unit=6) 
data = c.read(FC=3, ADR=287, LEN=5)
data_dict_old = {
    'ГПГУ 1 ': data[0],
    'ГПГУ 2 ': data[1],
    'ГПГУ 3 ': data[2],
    'ГПГУ 4 ': data[3],
    'ГПГУ 5 ': data[4]
}

while True:
    data = list(c.read(FC=3, ADR=287, LEN=5))
    data_dict_new = {
        'Дата Время':datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
        'ГПГУ 1 ': data[0],
        'ГПГУ 2 ': data[1],
        'ГПГУ 3 ': data[2],
        'ГПГУ 4 ': data[3],
        'ГПГУ 5 ': data[4]
        }
    for k in data_dict_old.keys():
        if not isinstance(data_dict_old[k], str):
            if data_dict_old[k] != 0 and data_dict_new[k] == 0:
                print(f'{k} остановлена {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
                bot.send_message(723253749, f'{k} остановлена {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
            if data_dict_old[k] == 0 and data_dict_new[k] != 0:
                print(f'{k} включена в работу {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')            
                bot.send_message(723253749, f'{k} включена в работу {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')

    with open(data_file, "a", newline='') as f:
        writer = csv.DictWriter(f, order)
        writer.writerow(data_dict_new)
    data_dict_old = data_dict_new
    time.sleep(1)   

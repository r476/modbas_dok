from modbus.client import *
import csv, time, datetime, telebot, json, requests, shutil, re, os.path
import pandas as pd
from pandas.tseries.offsets import Hour, Day
import matplotlib.pyplot as plt 

def get_updates():
    updates_list =[]
    return_json = []

    with open('req.json', 'r') as f:
        updates = json.load(f)
        for u in updates['result']:
            updates_list.append(u['update_id'])
    try:
        method = 'getUpdates'
        r = requests.get(f'https://api.telegram.org/bot{token}/{method}')

        for u in r.json()['result']:
            if u['update_id'] not in updates_list:
                return_json.append(u)

        with open('req.json', 'w') as f:
            json.dump(r.json(), f, indent=4)
    except Exception as e:
        print(e)
        
    return return_json

def get_id_list():
    with open('config.json', 'r') as f:
        return json.load(f)['accepted_id']
     
# определяю знак числа из модбас регистра
def number_sing(n):
    return (n-65535) if n & 0b1000000000000000 else n
    
# получаю данные с IM
def get_data():
    data_dict = {}
    try:
        c = client(host="192.168.127.254", unit=7)
        
        gensets = c.read(FC=3, ADR=287, LEN=5)
        mains_import = c.read(FC=3, ADR=231, LEN=1)[0]
        object_p = c.read(FC=3, ADR=272, LEN=2)[1]
        mwh = c.read(FC=3, ADR=283, LEN=2)[1]
        tot_run_p_act = c.read(FC=3, ADR=339, LEN=2)[1]
        b_in = c.read(FC=3, ADR=2, LEN=1)[0]
        data_dict = {'Дата Время':datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
            'ГПГУ 1 ': number_sing(gensets[0]), 
            'ГПГУ 2 ': number_sing(gensets[1]), 
            'ГПГУ 3 ': number_sing(gensets[2]), 
            'ГПГУ 4 ': number_sing(gensets[3]), 
            'ГПГУ 5 ': number_sing(gensets[4]), 
            'MainsImport': number_sing(mains_import), 
            'Мощность завода': number_sing(object_p), 
            'MWh': mwh, 
            'Сумм мощность ГПГУ': number_sing(tot_run_p_act), 
            'BIN': b_in}
    except Exception as e:
        print('Неудачная попытка опроса IM.')
        syslog_to_csv(e)
    return data_dict

# запись данных в CSV
def to_csv(data_file, order, data):
    with open(data_file, "a", newline='') as f:
        writer = csv.DictWriter(f, order)
        writer.writerow(data)

# запись в логи
def msglog_to_csv(text):
    with open('msglog.log', 'a', newline='') as f:
        f.write(str(text) + '\n')

def syslog_to_csv(text):
    with open('syslog.log', 'a', newline='') as f:
        f.write(str(text) + '\n')

# рассылка сообщений по списку ID
def send_messages(id_list, text):
    try:
        for i in id_list:
            url = f'https://api.telegram.org/bot{token}/sendMessage?chat_id={i}&text={text}&parse_mode=Markdown'
            r= requests.post(url)
    except:
        syslog_to_csv('Неудачная отправка сообщения')

def make_graph(mean_int, interval):
    try:
        df = pd.read_csv(data_file, parse_dates=['Дата Время'], index_col=['Дата Время'])

        data_mean = df.resample(mean_int).mean()
        data_sample = data_mean[data_mean.index[-1]-interval:]

        plt.figure(figsize=(12,6))
        plt.ylim([-1000, 7500])
        plt.ylabel('кВт')
    #    plt.xticks(rotation=45)
        plt.grid(True)
        plt.plot(data_sample.index, data_sample['Мощность завода'], 'r-')
        plt.plot(data_sample.index, data_sample['Сумм мощность ГПГУ'], 'g-')
        plt.plot(data_sample.index, data_sample['MainsImport'], 'b-')
        plt.axhline(y=data_sample['Мощность завода'].mean(), alpha=0.5, color='r')
        plt.axhline(y=data_sample['MainsImport'].mean(), alpha=0.5, color='b')
        plt.axhline(y=data_sample['Сумм мощность ГПГУ'].mean(), alpha=0.5, color='g')
        plt.legend(['Завод', 'ГПГУ', 'Импорт'])
        plt.figtext(.13, .96, f'Средняя мощность завода на выбранном интервале: {round(data_sample["Мощность завода"].mean())} кВт')
        plt.figtext(.13, .93, f'Средняя мощность ГПГУ на выбранном интервале: {round(data_sample["Сумм мощность ГПГУ"].mean())} кВт.   Выработано {int(data_sample["MWh"][-1]-data_sample["MWh"][0])} МВт ч')
        plt.figtext(.13, .9, f'Средний импорт на выбранном интервале: {round(data_sample["MainsImport"].mean())} кВт')
        plt.savefig('1.png')
    except Exception as e:
        shutil.copyfile('fail.png', '1.png')
        syslog_to_csv(e)
    
# отправка отчета в виде графика с подписями
def send_report(id_list):
    # Формирую график за сутки -------------------------------------
    try:
        make_graph('2T', Hour(24))
        for i in id_list:
            url = f"https://api.telegram.org/bot{token}/sendPhoto"
            files = {'photo': open('1.png', 'rb')}
            data = {'chat_id' : i}
            requests.post(url, files=files, data=data)
            
        with open('config.json', 'r') as f:
            jdata = json.load(f)
            
        with open('config.json', 'w') as f:
            jdata['report_today'] = True
            json.dump(jdata, f, indent=4)

    except Exception as e:
        syslog_to_csv('\n\nНеудачная отправка send_report\n\n')
        syslog_to_csv(e)
    # Формирую суточный отчёт в файл DOC_report24.csv
    try:
        data_dict = {}
        report_file_name = 'DOC_report24.csv'
        if not os.path.exists(report_file_name):
                with open(report_file_name, 'w') as f:
                    f.write(','.join(['DateTime', 'ObjectConsuption', 'MachinesPower', 'Import', 'MWh']) + '\n')

        df = pd.read_csv(data_file, parse_dates=['Дата Время'], index_col=['Дата Время'])
        data_sample = df[df.index[-1]-Hour(24):]
        data_dict['DateTime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        data_dict['ObjectConsuption'] = int(data_sample['Мощность завода'].mean())
        data_dict['MachinesPower'] = int(data_sample['Сумм мощность ГПГУ'].mean())
        data_dict['Import'] = int(data_sample['MainsImport'].mean())
        data_dict['MWh'] = data_sample['MWh'][-1] - data_sample['MWh'][0]
        to_csv(report_file_name, ['DateTime', 'ObjectConsuption', 'MachinesPower', 'Import', 'MWh'], data_dict)        
    except Exception as e:
        syslog_to_csv('\n\nНеудачное сохранение суточного отчёта в файл DOC_report24.csv\n\n')
        syslog_to_csv(e)
        
        
def reset_report_marker():
    with open('config.json', 'r') as f:
        jdata = json.load(f)
    if jdata['report_today']:
        with open('config.json', 'w') as f:
            jdata['report_today'] = False
            json.dump(jdata, f, indent=4)
            
def is_report_marker_on():
    with open('config.json', 'r') as f:
        jdata = json.load(f)
        return False if not jdata['report_today'] else True
    
def send_graph(message_id):
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    files = {'photo': open('1.png', 'rb')}
    data = {'chat_id' : message_id}
    try:
        requests.post(url, files=files, data=data)
    except Exception as e:
        syslog_to_csv(e)
        
def send_document(user_id, file):
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    files = {'document': open(file, 'rb')}
    data = {'chat_id' : user_id}
    try:
        requests.post(url, files=files, data=data)
    except Exception as e:
        syslog_to_csv(e)
    
def handler_updates(message):
    pattern_hours = r'\/(\d{1,2})h' # шаблон для определения количества часов в интервале
    pattern_days = r'\/(\d{1,2})d' # шаблон для определения количества дней в интервале
    add_id_pattern = r'\/add_id_(\d*)' # шаблон для выделения ID из запроса
    message_id = message['message']['from']['id']
    text = message['message']['text']
    msg_report = f"{datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')}. Запрос от {message['message']['from']['first_name']}, id: {message['message']['from']['id']}\n{message['message']['text']}\n"
    msglog_to_csv(msg_report)
    syslog_to_csv(message_id)
    syslog_to_csv(text)
    
    if text == '/wtf':
        df = pd.read_csv(data_file, parse_dates=['Дата Время'], index_col=['Дата Время'])[-5:]
        text = f"*{datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')}*\n\n*ГПГУ 1: *{int(df['ГПГУ 1 '].mean())} кВт\n*ГПГУ 2: *{int(df['ГПГУ 2 '].mean())} кВт\n*ГПГУ 3: *{int(df['ГПГУ 3 '].mean())} кВт\n*ГПГУ 4: *{int(df['ГПГУ 4 '].mean())} кВт\n*ГПГУ 5: *{int(df['ГПГУ 5 '].mean())} кВт\n\n*Мощность завода: *{int(df['Мощность завода'].mean())} кВт\n*Сумм мощность ГПГУ: *{int(df['Сумм мощность ГПГУ'].mean())} кВт\n*Импорт: *{int(df['MainsImport'].mean())} кВт\n\n*MWh: *{df['MWh'][-1]}"
        send_messages([message_id], text)

    if text == '/get_csv':
        send_document(message_id, data_file)
        
    if text == '/get_report':
        send_document(message_id, 'DOC_report24.csv')

    if text == '/get_msglog':
        send_document(message_id, 'msglog.log')
        
    if text == '/get_syslog':
        send_document(message_id, 'syslog.log')

    # совпадение с шаблоном добавления ID в формате: /add_id_*********
    add_id = re.match(add_id_pattern, text)
    if add_id and message_id==723253749: 
        user_id = int(add_id.group(1))
        with open('config.json', 'r') as f:
            j = json.load(f)
        if user_id not in j['accepted_id']:
            j['accepted_id'].append(user_id)
            send_messages([723253749], f'Пользователь с id: {user_id} успешно добавлен в рассылку')
        else:
            send_messages([723253749], f'Пользователь с id: {user_id} уже есть в рассылке')
        with open('config.json', 'w') as f:
            json.dump(j, f, indent=4)
        id_list = get_id_list()

    # совпадение с шаблоном добавления ID в формате: /del_me
    if text == '/del_me':
        try:
            with open('config.json', 'r') as f:
                j = json.load(f)
                j['accepted_id'].remove(message_id)
                send_messages([message_id], f'Пользователь с id: {message_id} успешно удалён из рассылки')
            with open('config.json', 'w') as f:
                json.dump(j, f, indent=4)
            id_list = get_id_list()
        except:
            send_messages([message_id], f'Пользователь с id: {message_id} отсутствует в рассылке')

    # совпадение с шаблоном запроса часового интервала в формате: /get_data_in_3_hours
    get_hours = re.match(pattern_hours, text)
    if get_hours: 
        h = int(get_hours.group(1))
        make_graph('1T', Hour(h))
        send_graph(message_id)

    # совпадение с шаблоном запроса часового интервала в формате: /get_data_in_3_days
    get_days = re.match(pattern_days, text)
    if get_days: 
        d = int(get_days.group(1))
        make_graph('2T', Day(d))
        send_graph(message_id)

    # Оповещалка о запросах
    if message['message']['from']['id'] != 723253749:
        send_messages([723253749], msg_report)

id_list = get_id_list()

titles = ['Дата Время', 
          'ГПГУ 1 ', 
          'ГПГУ 2 ', 
          'ГПГУ 3 ', 
          'ГПГУ 4 ', 
          'ГПГУ 5 ', 
          'MainsImport', 
          'Мощность завода', 
          'MWh', 
          'Сумм мощность ГПГУ', 
          'BIN']

token = '1298999210:AAHQXHgqW0y0A9kjCPB3XSeBZKDNrgmK9fY'
#token = '827576612:AAEX0IHqMW5x-oWrh8T1ZXhE-9_K8pXMTJ0'
data_file = "data.csv"

# блок кода для инициации файла csv
#with open(data_file, 'w') as f:
#    writer = csv.writer(f)
#    writer.writerow(titles)

while not get_data():
    time.sleep(1)
    
data_dict_old = get_data()

# Основной цикл
while True:
    data_dict_new = get_data() if get_data() else data_dict_old # если данные не получены, то оставляю старые значения 
    
    for k in ['ГПГУ 1 ', 'ГПГУ 2 ', 'ГПГУ 3 ', 'ГПГУ 4 ', 'ГПГУ 5 ']:
        if data_dict_old[k] > 0 and data_dict_new[k] == 0:
            text = f'{k} остановлена {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}'
            send_messages(id_list, text)
            syslog_to_csv(text)
         
        if data_dict_old[k] == 0 and data_dict_new[k] > 0:
            text = f'{k} включена в работу {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}'
            send_messages(id_list, text)
            syslog_to_csv(text)
            
    if data_dict_old['BIN'] & 1 and not (data_dict_new['BIN'] & 1): # бит 1 с 1 на 0
        text = f'МСВ разомкнут, работаем в острове. {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}'
        send_messages(id_list, text)
        syslog_to_csv(text)

    if not (data_dict_old['BIN'] & 1) and data_dict_new['BIN'] & 1: # бит 1 с 0 на 1
        text = f'МСВ замкнут, работаем в нормальном режиме. {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}'
        send_messages(id_list, text)
        syslog_to_csv(text)

    # Отправка ежедневного отчёта
    if datetime.datetime.now().hour==19:
        if not is_report_marker_on():
            send_report(id_list)

    # Сброс маркера отчёта в полночь
    if datetime.datetime.now().hour==0:
        if is_report_marker_on():
            reset_report_marker()

    to_csv(data_file, titles, data_dict_new)
    data_dict_old = data_dict_new
    
    #обработка telegram запросов 
    updates = get_updates()
    if updates:
        for message in updates:
            handler_updates(message)
    
    time.sleep(10)

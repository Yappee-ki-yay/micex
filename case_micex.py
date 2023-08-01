#@title Импорты
from selenium import webdriver
import time
from selenium.webdriver.common.by import By
import datetime as dt
from selenium.webdriver.support.ui import Select
import pandas as pd
import sqlite3
import csv
import matplotlib.pyplot as plt
%matplotlib inline
import time
import numpy as np
import os
pd.set_option('display.max_rows', None)
pd.options.mode.chained_assignment = None



  
#@title Скачивание csv
now = dt.date.today()
russ_month = ["Янв","Фев","Мар","Апр","Май","Июн","Июл","Авг","Сен","Окт","Ноя","Дек"]
options = webdriver.ChromeOptions()
options.add_argument("--disable-extensions")
options.add_argument("--no-sandbox")
options.add_argument("--disable-notifications")
options.add_argument("--disable-popup-blocking")
options.add_argument('--start-maximized')
prefs = {"profile.default_content_settings.popups" : 0,
"download.default_directory" : os.getcwd(),
"directory_upgrade" : True }
options.add_experimental_option( "prefs" , prefs)
url = "https://www.finam.ru/profile/mirovye-indeksy/micex/export/"
driver = webdriver.Chrome('goog', options=options)
try:
    driver.get(url=url)
    time.sleep(10)
    
    driver.find_element(By.XPATH, '//*[@id="issuer-profile-export-first-row"]/td[3]/div/div[1]').click()
    time.sleep(1)
    driver.find_element(By.LINK_TEXT, "1 день").click()
    time.sleep(1)
    driver.find_element(By.XPATH, '//*[@id="issuer-profile-export-second-row"]/td[3]/div/div[1]').click()
    time.sleep(1)
    driver.find_element(By.LINK_TEXT, ".csv").click()
    time.sleep(1)
    driver.find_element(By.XPATH, '//*[@id="issuer-profile-export-separator-row"]/td[3]/div/div[1]').click()
    time.sleep(1)
    driver.find_element(By.LINK_TEXT, "запятая (,)").click()
    time.sleep(1)
    driver.find_element(By.XPATH, '//*[@id="issuer-profile-export-separator-row"]/td[5]/div/div[1]').click()
    time.sleep(1)
    driver.find_element(By.LINK_TEXT, "нет").click()
    time.sleep(1)
  
    driver.find_element(By.XPATH, '//*[@id="issuer-profile-export-from-control"]').click()
    time.sleep(1)
    driver.find_element(By.XPATH, f'//*[@id="ui-datepicker-div"]/div/div/select[2]//option[text()="{now.year - 5}"]').click()
    time.sleep(1)
    driver.find_element(By.XPATH, f'//*[@id="ui-datepicker-div"]/div/div/select[1]//option[text()="{russ_month[now.month - 1]}"]').click()
    time.sleep(1)
    try:
        a = now.day
        driver.find_element(By.LINK_TEXT, f"{a}").click()
    except:
        a -= 1
        try:
            driver.find_element(By.LINK_TEXT, f"{a}").click()
        except:
            a -= 1
            try:
                driver.find_element(By.LINK_TEXT, f"{a}").click()
            except:
                a -= 1
                try:
                    driver.find_element(By.LINK_TEXT, f"{a}").click()
                except:
                    print('Упс, что-то пошло не так')
    time.sleep(1)
  
    driver.find_element(By.XPATH, '//*[@id="issuer-profile-export-to"]').click()
    time.sleep(1)
    driver.find_element(By.XPATH, f'//*[@id="ui-datepicker-div"]/div/div/select[2]//option[text()="{now.year}"]').click()
    time.sleep(1)
    driver.find_element(By.XPATH, f'//*[@id="ui-datepicker-div"]/div/div/select[1]//option[text()="{russ_month[now.month - 1]}"]').click()
    time.sleep(1)
    try:
        a = now.day
        driver.find_element(By.LINK_TEXT, f"{a}").click()
    except:
        a -= 1
        try:
            driver.find_element(By.LINK_TEXT, f"{a}").click()
        except:
            a -= 1
            try:
                driver.find_element(By.LINK_TEXT, f"{a}").click()
            except:
                a -= 1
                try:
                    driver.find_element(By.LINK_TEXT, f"{a}").click()
                except:
                    print('Упс, что-то пошло не так')
    time.sleep(1)
    driver.find_element(By.XPATH, '//*[@id="issuer-profile-export-button"]/button').click()
    time.sleep(10)
except Exception as ex:
    print(ex)
finally:
    driver.close()
    driver.quit()




  
#@title Создание базы данных
strYear = str(now.year)
strYear_1 = str(now.year - 5)

conn = sqlite3.connect(f'IMOEX_{strYear_1[2:4]}{now.strftime("%m")}{a}_{strYear[2:4]}{now.strftime("%m")}{now.day}.db')
cur = conn.cursor()

def table_exists(table_name): 
    cur.execute('''SELECT count(name) FROM sqlite_master WHERE TYPE = 'table' AND name = "{}" '''.format(table_name)) 
    if cur.fetchone()[0] == 1: 
        return True 
    return False

if not table_exists('t'): 
    cur.execute(''' 
        CREATE TABLE t (TICKER, PER, DATE, TIME, OPEN, HIGH, LOW, CLOSE, VOL); 
    ''')
    with open(f'IMOEX_{strYear_1[2:4]}{now.strftime("%m")}{a}_{strYear[2:4]}{now.strftime("%m")}{now.day}.csv','r') as fin:
        dr = csv.DictReader(fin, delimiter=',')
        to_db = [(i['<TICKER>'], i['<PER>'], i['<DATE>'], i['<TIME>'], i['<OPEN>'], i['<HIGH>'], i['<LOW>'], i['<CLOSE>'], i['<VOL>']) for i in dr]
    cur.executemany("INSERT INTO t (TICKER,PER,DATE,TIME,OPEN,HIGH,LOW,CLOSE,VOL) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", to_db)
    conn.commit( )


      
#@title Подготовка DataFrame для первой аналитики
df = pd.read_sql('SELECT DATE, TIME, CLOSE FROM t', conn)
df['DATE'] = df['DATE'].astype('datetime64[ns]')
df['CLOSE'] = df['CLOSE'].astype(float)
df = pd.concat([df, pd.DataFrame.from_records([{ 'DATE': None, 'CLOSE': None}])], ignore_index=True)
df['year_month'] = df['DATE'].dt.to_period("M")
df['CLOSE_1'] = df['CLOSE']
df['CLOSE_1'] = df['CLOSE_1'].shift(1)
df['max_close'] = df['CLOSE'].groupby(df['year_month']).transform('max')
df['max_close_1'] = df['CLOSE_1'].groupby(df['year_month']).transform('max')
df['max'] = np.where(df['max_close'] >= df['max_close_1'], df['max_close'], df['max_close_1'])
df = df.drop(['max_close','max_close_1'], axis=1)
df['min_close'] = df['CLOSE'].groupby(df['year_month']).transform('min')
df['min_close_1'] = df['CLOSE_1'].groupby(df['year_month']).transform('min')
df['min'] = np.where(df['min_close'] > df['min_close_1'], df['min_close_1'], df['min_close'])
df = df.drop(['min_close','min_close_1'], axis=1)
df['means'] = df['CLOSE'].groupby(df['year_month']).transform('mean')
df = df.drop(['TIME'], axis=1)
df.insert(len(df.columns), 'devi', 0)
df['devi'] = df['devi'].astype(float)
df.insert(len(df.columns), 'devi_100', 0)
df['devi_100'] = df['devi_100'].astype(float)
df.insert(len(df.columns), 'devi_mon', 0)
df['devi_mon'] = (df['devi_mon']).astype(float)
df['devi'] = (df['CLOSE'] - df['means']).abs()
df['devi'] = (df['devi']).groupby(df['year_month']).transform('mean')
df['devi_100'] = np.where(df['devi'] > 100, np.float64(df['devi']), np.float64(0))
df = df.reset_index()
df = df.drop(['index'], axis=1)
conn.close()



#@title Расчёт
tic = time.perf_counter()
df_final = df.drop(columns=['DATE']).groupby('year_month').mean().reset_index()
df_final['devi_100'] = np.where(df_final['devi_100'] == 0, None, df_final['devi_100'])
df_final = df_final.dropna()
df_final['devi_100'] = df_final['devi_100'].astype('float')
df_final['devi_100'] = round(df_final['devi_100'], 2)
df_final.rename(columns = {'year_month':'Год-месяц', 'devi_100':'Среднемесячное отклонение'}, inplace = True)
dates = list(df_final['Год-месяц'])
for i in range (len(dates)):
    dates[i] = dates[i].strftime('%y-%m')
toc = time.perf_counter()



#@title Результат первой аналитики
df_final[['Год-месяц','Среднемесячное отклонение']]




#@title Построение графика
plt.figure(figsize=(8, 4))
plt.bar(dates, list(df_final['Среднемесячное отклонение']), color='#87CEFA', )
plt.title('График зависимости дельты\nсреднемесячного отклонения\nот времени')
plt.xlabel('Год-месяц')
plt.ylabel('Среднемесячное отклонение')
for x, y, i in zip(dates, list(df_final['Среднемесячное отклонение']), list(df_final['Среднемесячное отклонение'])):
  plt.text(x, y, i,
           fontsize=16,
           horizontalalignment='center',
           verticalalignment='top',
           color='#000080')
plt.show()



#@title Начало выполнения второй аналитики (создание нового DataFrame)
df_2 = df.drop(['means','devi', 'devi_100', 'devi_mon', 'CLOSE_1'], axis=1)
df_2 = pd.concat([df_2, pd.DataFrame.from_records([{ 'DATE': None, 'CLOSE': None, 'year_month':None, 'max':None, 'min':None}])], ignore_index=True)
df_2 = pd.concat([df_2, pd.DataFrame.from_records([{ 'DATE': None, 'CLOSE': None, 'year_month':None, 'max':None, 'min':None}])], ignore_index=True)
df_2 = pd.concat([df_2, pd.DataFrame.from_records([{ 'DATE': None, 'CLOSE': None, 'year_month':None, 'max':None, 'min':None}])], ignore_index=True)
df_2 = pd.concat([df_2, pd.DataFrame.from_records([{ 'DATE': None, 'CLOSE': None, 'year_month':None, 'max':None, 'min':None}])], ignore_index=True)
# df_2 = df_2.append({'DATE':None, 'CLOSE':None, 'year_month':None, 'max':None, 'min':None}, '0')
# df_2 = df_2.append({'DATE':None, 'CLOSE':None, 'year_month':None, 'max':None, 'min':None}, '0')
# df_2 = df_2.append({'DATE':None, 'CLOSE':None, 'year_month':None, 'max':None, 'min':None}, '0')
# df_2 = df_2.append({'DATE':None, 'CLOSE':None, 'year_month':None, 'max':None, 'min':None}, '0')
df_2 = df_2.shift()
df_2 = df_2.shift()



#@title Расчёт
tic = time.perf_counter()

df_2['CLOSE_1'] = df_2['CLOSE']
df_2['CLOSE_1'] = df_2['CLOSE_1'].shift(1)

df_2['CLOSE_2'] = df_2['CLOSE']
df_2['CLOSE_2'] = df_2['CLOSE_2'].shift(-1)

df_2['breakdown'] = np.where((df_2['CLOSE'] - df_2['CLOSE_2']) >= 0.2*(df_2['max'] - df_2['min']), 'down', 'ok')
df_2['breakdown'] = np.where((-1)*(df_2['CLOSE'] - df_2['CLOSE_2']) >= 0.2*(df_2['max'] - df_2['min']), 'up', df_2['breakdown'])
df_2['boost'] = df_2['breakdown'].copy()

df_2['boost'] = np.where((df_2['boost'] == 'down') & (df_2['CLOSE'] <= df_2['CLOSE_1']) & (df_2['CLOSE'] > df_2['CLOSE_2']), 'boost down', df_2['boost'])
df_2['boost'] = np.where((df_2['boost'] == 'up') & (df_2['CLOSE'] >= df_2['CLOSE_1']) & (df_2['CLOSE'] < df_2['CLOSE_2']), 'boost up', df_2['boost'])
df_2['boost'] = np.where((df_2['boost'] == 'down') & (df_2['CLOSE'] >= df_2['CLOSE_1']) & (df_2['CLOSE'] > df_2['CLOSE_2']), 'local max', df_2['boost'])
df_2['boost'] = np.where((df_2['boost'] == 'up') & (df_2['CLOSE'] <= df_2['CLOSE_1']) & (df_2['CLOSE'] < df_2['CLOSE_2']), 'local min', df_2['boost'])

df_2['CLOSE_1'] = df_2['CLOSE_1'].shift(-1)
df_2['CLOSE_2'] = df_2['CLOSE_2'].shift(1)
df_2 = df_2.drop(['CLOSE_1','CLOSE_2'], axis=1)

df_2 = df_2.dropna()
toc = time.perf_counter()




#@title Результат второй аналитики
df_2_print = df_2[['year_month','DATE','CLOSE','max', 'min', 'boost']]
df_2_print.rename(columns = {'year_month':'Год-месяц', 'DATE':'Дата', 'max':'Макс. цена', 'min':'Мин. цена', 'boost':'Тип старта'}, inplace = True)
df_3_print = df_2_print.copy()
df_3_print['CLOSE'] = np.where((df_3_print['Тип старта'] == 'boost down') | (df_3_print['Тип старта'] == 'boost up') | (df_3_print['Тип старта'] == 'local max') | (df_3_print['Тип старта'] == 'local min'), df_3_print['CLOSE'], None)

df_3_print = df_3_print.dropna().reset_index()
df_3_print = df_3_print.drop(['index'], axis=1)

df_3_print




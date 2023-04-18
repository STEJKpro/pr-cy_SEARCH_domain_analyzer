import json
import configparser
import os
import requests
from pathlib import Path
import sqlite3
import pandas
from math import ceil
import datetime


class SearchAnalyzer():
    session = requests.Session()

    __BASE_API_URL = 'https://line.pr-cy.ru/api/v1.1.0'
    low_price = 0.25
    high_price = 0.3

    __tasks_resaults = []


    def __init__(self, *, key: str = None, use_config: bool = True, search_engine: str, search_type: str, search_depth: str, file_path: str, search_region: int = 1) -> None:
        if use_config:
            config = configparser.ConfigParser()
            config.read(os.path.join('cfg', 'config.ini'))
            self.__API_KEY = config['DEFAULT']['API_KEY']
            self.low_price = config['DEFAULT']['low_price']
            self.height_price = config['DEFAULT']['high_price']
            if search_engine == 'yandex':
                self.region_id = config['DEFAULT']['REGION_ID_Yandex']
            if search_engine == 'google':
                self.region_id = config['DEFAULT']['REGION_ID_Google']

        else:
            if not key:
                raise ValueError(
                    'Должен быть указан API ключ при инициализации класа, либо же использоваться use_config = True')
            else:
                self.__API_KEY = key

        self.search_engine = search_engine
        self.search_type = search_type
        self.search_depth = search_depth
        if Path(file_path).is_file():
            self.query_file_path = file_path
        else:
            raise FileExistsError('Файл не найден!')

    def check_balanc(self, keywords):
        balance = self.get_balance(self.session)
        if len(keywords) > 100:
            if (balance - (len(keywords)*self.low_price)) > 0:
                print(
                    f'Баланс составляет: {balance}, стоимость текущего задания ~ { len(keywords)*self.low_price }')
                return True
            else:
                print(
                    f'Баланс составляет: {balance}, стоимость текущего задания ~ { len(keywords)*self.low_price }')
                return False

        if len(keywords) <= 100:
            if (balance - (len(keywords)*self.high_price)) > 0:
                print(
                    f'Баланс составляет: {balance}, стоимость текущего задания ~ { len(keywords)*self.high_price }')
                return True
            else:
                print(
                    f'Баланс составляет: {balance}, стоимость текущего задания ~ { len(keywords)*self.high_price }')
                return False

    def get_balance(self,):
        response = self.session.get(
            self.__BASE_API_URL+'/user/balance', params={'key': self.__API_KEY})
        if response.status_code == 200:
            return json.loads(response.text)['balance']
        else:
            print(f"Вохникла ошибка: {json.loads(response.text).get('error')}")
            return None

    def creat_task(self, keywords):
        """Принимает до 1000 ключевых запросов"""
        if len(keywords) >=1000:
            raise ValueError('Передано слишком много ключей на проверку')
        
        response = self.session.post(self.__BASE_API_URL+'/task/create', params = {'key' : self.__API_KEY},
                    data = {
                    'keywords[]': keywords, 
                    'engine' : self.search_engine,
                    'regionId' : self.region_id,
                    'device' : self.search_type,
                    'language' : 'ru'
                    })
        if response.status_code == 200:
            taskId = json.loads(response.text)['taskId']
            return taskId
        else:
            raise Exception(f'Ошибка создания задачи\n{response.status_code}\n{json.loads(response.text)}')

    def task_status_check(self, taskId):
        response = self.session.get(self.__BASE_API_URL+ f'/task/status/{taskId}', params = {'key': self.__API_KEY})
        if response.status_code == 200:
            status = json.loads(response.text)['status']
            if status == 'done':
                return True 
            else: 
                print(f"Статус задачи {taskId}: {status}")
                return False
        else:
            raise Exception(f'Ошибка проверки статуса задачи\n{response.status_code}\n{json.loads(response.text)}')

    def get_keywords_from_file(self) -> list:
        """return : Возращает список со списками ключевых слов из файла с количеством ключевиков до 1000 шт"""
        df = pandas.read_excel(self.query_file_path, header=None)
        data = df.iloc[ : , 0].tolist()
        self.keywords_count = len(data)
        self.keywords = []
        if len(data) >=1000:
            n = ceil( len(data)/ ceil(len(data)/999))
            for x in range(0, len(data), n):
                e_c = data[x : n + x]
                self.keywords.append(e_c)
        else:
            self.keywords.append(data)
        return self.keywords

    def get_task_resault(self, taskId):
        response = self.session.get(self.__BASE_API_URL+f'/task/result/{taskId}', params = {'key':self.__API_KEY})
        if response.status_code == 200:
            result = json.loads(response.text)
            self.__tasks_resaults.append(result)
            return result
        else:
            raise ConnectionError(f'Не удалось получить результаты {taskId}')
            
    @property
    def tasks_resaults(self):
        return [item for item in self.__tasks_resaults]
    
    def get_distinct_hosts(self):
        distinct_hosts = []
        for task_resault in self.__tasks_resaults:
            for keyword in task_resault['keywords']:
                for resault in keyword['serp'][:self.search_depth]:
                    host = resault['host'].replace('www.', '').lower()
                    if host not in distinct_hosts:
                        distinct_hosts.append(host)
        return distinct_hosts
    
    def get_query_resaults(self):
        self.final_data = []
        for task_resaults in self.__tasks_resaults:
            for keyword in task_resaults['keywords']:
                for resault in keyword['serp'][:self.search_depth]:
                    res={}
                    res['query'] = keyword['query']
                    res['position'] = resault['position']
                    res['url'] = resault['url']
                    res['host'] = resault['host']
                    res['title'] = resault['title']
                    self.final_data.append(res)
        
        return self.final_data

    def get_distinct_hosts_statistic(self, ):
        result = []
        for distinct_host in self.get_distinct_hosts():
            counter = 0
            for task_resaults in self.tasks_resaults:
                for keyword in task_resaults['keywords']:
                    for resault in keyword['serp'][:self.search_depth]:
                        if distinct_host.lower() in resault['host'].lower():
                            counter +=1
                            break;
            result.append(
                (distinct_host.lower(), round((counter / self.keywords_count)*100, 2))
            )
        return result
    
    def get_distinct_hosts_statistic_top10(self, ):
        result = []
        for distinct_host in self.get_distinct_hosts():
            counter = 0
            for task_resaults in self.tasks_resaults:
                for keyword in task_resaults['keywords']:
                    for resault in keyword['serp'][:10]:
                        if distinct_host.lower() in resault['host'].lower():
                            counter +=1
                            break;
            result.append(
                (distinct_host.lower(), round((counter / self.keywords_count)*100, 2))
            )
        return result
        

class DomainAnalyzer():

    session = requests.Session()

    __BASE_API_URL = 'https://apis.pr-cy.ru/api/v1.1.0'
    __tasks_resaults = []

    api_query_count = 0


    def __init__(self, *, key: str = None, use_config: bool = True, file_path: str = None ) -> None:
        if use_config:
            config = configparser.ConfigParser()
            config.read(os.path.join('cfg', 'config.ini'))
            self.__API_KEY = config['DEFAULT']['DOMAIN_API_KEY'] 
            self.google_spreadsheet_url = config['DEFAULT']['Google_spreadsheet_url'] + '/export?format=xlsx'
            self.FRESHNESS = int(config['DEFAULT']['FRESHNESS_days'])

        else:
            if not key:
                raise ValueError(
                    'Должен быть указан API ключ при инициализации класа, либо же использоваться use_config = True')
            else:
                self.__API_KEY = key

        if Path(file_path).is_file():
            self.query_file_path = file_path
        else:
            raise FileExistsError('Файл не найден!')


    @property
    def tasks_resaults(self):
        return self.__tasks_resaults
    

    def get_domains_from_file(self) -> list:
        df = pandas.read_excel(self.query_file_path, header=None)
        data = df.iloc[ : , 0].tolist()
        self.domains_count = len(data)
        self.domains = data
        if self.domains_count >= 200:
            print(f'ПРЕДУПРЕЖДЕНИЕ! Количество доменов для анализа привышает 200 ({self.domain_count}), \nОграничение api: 200 запросов в час, обработка может занять много времени')
        return data


    def get_data_from_google_sheet(self):
        # with sqlite3.connect('local.db') as connection:
            # create a cursor object from the cursor class
            # cur = connection.cursor()
        df = pandas.read_excel(self.google_spreadsheet_url, skiprows=1)
        return df


    def update_domain_analysis(self, domain):
        url = f'/analysis/update/base/{domain}'
        response = requests.post(self.__BASE_API_URL+url, params={'key':self.__API_KEY})
        self.api_query_count+=1
        if response.status_code != 200:
            print(f'Ошибка при попытке отправки запроса на обновление {domain}')
            return False

    def get_domain_analysis(self, domain):
        url = f'/analysis/base/{domain}'
        response = requests.get(self.__BASE_API_URL+url, 
                                params={
                                            'key': self.__API_KEY, 
                                            'tests':'avgVisitDuration,googleIndex,ip,mainPageExternalLinks,megaindexLinksCount,pageSpeedDesktop,pageSpeedMobile,pagesPerVisit,publicStatistics,sitemap,trafficGeography,trafficSources,whoisCreationDate,yandexAchievements,yandexIndex,yandexReviews',
                                            'excludeHistory': 1
                                        }
                                ) 
        self.api_query_count+=1
        if response.status_code == 429:
            return False
        data = json.loads(response.text)
        data['host'] = domain
        self.__tasks_resaults.append(data)
        return data

    def check_domain_status(self, domain):
        url = f'/analysis/status/base/{domain}'
        response = requests.get(self.__BASE_API_URL+url, params={'key': self.__API_KEY})
        response_content = json.loads(response.text)
        if response.status_code != 200:
            return (False, 'new')
        if not response_content.get('updated'):
            return (False, 'updating')
        if datetime.datetime.strptime(response_content.get('updated'), "%Y-%m-%dT%H:%M:%S+03:00") >= datetime.datetime.now()- datetime.timedelta(days=self.FRESHNESS) and \
                not response_content.get('isUpdating'):
            return (True, 'Ок')
        if datetime.datetime.strptime(response_content.get('updated'), "%Y-%m-%dT%H:%M:%S+03:00") <= datetime.datetime.now()- datetime.timedelta(days=self.FRESHNESS):
            return (False, 'overdue')
        if response_content.get('isUpdating'):
            return (False, 'updating')

    def get_final_records(self):
        google_df = self.get_data_from_google_sheet()
        final_data = []
        for host_info in self.__tasks_resaults:

            res={}
            res['host'] = host_info['host']
            try: res['googleIndex'] = host_info['googleIndex']['googleIndex']
            except: res['googleIndex'] = ''
            try: res['yandexIndex'] = host_info['yandexIndex']['yandexIndex']
            except: res['yandexIndex'] = ''   
            try: res['Длина визита, сек'] = host_info['avgVisitDuration']['avgVisitDuration']
            except: res['Длина визита, сек'] = ''
            try: res['ip'] = host_info['ip']['ip'] 
            except: res['ip'] = ''
            try: res['Кол-во внешних ссылок по Мегаиндекс'] = host_info['megaindexLinksCount']['megaindexLinksCount']
            except: res['Кол-во внешних ссылок по Мегаиндекс'] = ''
            try: res['ПК pageSpeed оценка'] = host_info['pageSpeedDesktop']['pageSpeed']['score']
            except: res['ПК pageSpeed оценка'] = ''
            try: res['ПК pageSpeed в секундах'] = host_info['pageSpeedDesktop']['pageSpeed']['value']
            except:  res['ПК pageSpeed в секундах'] =''
            try: res['Мобильный pageSpeed оценка'] = host_info['pageSpeedMobile']['pageSpeed']['score']
            except:  res['Мобильный pageSpeed оценка']  = ''
            try: res['Мобильный pageSpeed в секундах'] = host_info['pageSpeedMobile']['pageSpeed']['value']
            except: res['Мобильный pageSpeed в секундах'] = ''
            try: res['Просмотры в мес'] = host_info['publicStatistics']['publicStatisticsPageViewsMonthly']
            except: res['Просмотры в мес'] = ''
            try: res['Посетители в мес'] = host_info['publicStatistics']['publicStatisticsPrcyVisitsMonthly']
            except: res['Посетители в мес'] = ''
            try: res['sitemap'] = host_info['sitemap']['sitemapUrl']
            except: res['sitemap'] = ''
            try: res['Трафик по странам'] = host_info['trafficGeography']['topCountryGeography'] 
            except: res['Трафик по странам'] = ''
            try: res['Ист. траф. Прямые заходы'] = host_info['trafficSources']['trafficSourcesDirect']/100
            except: res['Ист. траф. Прямые заходы'] = ''    
            try: res['Ист. траф. Почтовые рассылки'] = host_info['trafficSources']['trafficSourcesMail']/100
            except: res['Ист. траф. Почтовые рассылки'] = ''
            try: res['Ист. траф. Ссылки на сайтах'] = host_info['trafficSources']['trafficSourcesReferrals']/100
            except:  res['Ист. траф. Ссылки на сайтах'] =''
            try: res['Ист. траф. Поисковые системы'] = host_info['trafficSources']['trafficSourcesSearch']/100
            except: res['Ист. траф. Поисковые системы'] = '' 
            try: res['Ист. траф. Соц сети'] = host_info['trafficSources']['trafficSourcesSocial']/100
            except: res['Ист. траф. Соц сети'] = ''
            try: res['Дата создания сайта'] = datetime.datetime.strptime(host_info['whoisCreationDate']['whoisCreationDate'], '%Y-%m-%dT%H:%M:%S+03:00')
            except: res['Дата создания сайта'] = ''  
            try: res['Сайт на https?'] = host_info['yandexAchievements']['yandexAchievementsHttps']
            except: res['Сайт на https?']  = '' 
            try: res['Есть Турбо?'] = host_info['yandexAchievements']['yandexAchievementsTurbo']
            except: res['Есть Турбо?'] = ''     
            try: res['Отзывов в Яндекс'] = host_info['yandexReviews']['count']
            except: res['Отзывов в Яндекс'] = '' 
            try: res['Внешних ссылок на главную'] = host_info['mainPageExternalLinks']['externalIndexCount']
            except: res['Внешних ссылок на главную'] = ''
            try: res['Наш партнер?'] = google_df.loc[google_df['Домен'] == host_info['host']].iloc[0]['Наш партнер?']
            except: res['Наш партнер?'] = ''
            try:res['Ответственный менеджер'] = google_df.loc[google_df['Домен'] == host_info['host']].iloc[0]['Ответственный менеджер']
            except: res['Ответственный менеджер'] = ''
            try:res['Тип партнера'] = google_df.loc[google_df['Домен'] == host_info['host']].iloc[0]['Тип партнера']
            except:res['Тип партнера'] =''
                

            final_data.append(res)
            
        return final_data
    


class SearchRepeater():
    __tasks_resaults = []
    __BASE_API_URL = 'https://line.pr-cy.ru/api/v1.1.0'
    session = requests.Session()

    def __init__(self, search_depth, keywords_count):
        config = configparser.ConfigParser()
        config.read(os.path.join('cfg', 'config.ini'))
        self.__API_KEY = config['DEFAULT']['API_KEY']
        self.search_depth = search_depth
    
        self.keywords_count = keywords_count

    def task_status_check(self, taskId):
        response = self.session.get(self.__BASE_API_URL+ f'/task/status/{taskId}', params = {'key': self.__API_KEY})
        if response.status_code == 200:
            status = json.loads(response.text)['status']
            if status == 'done':
                return True 
            else: 
                print(f"Статус задачи {taskId}: {status}")
                return False
        else:
            raise Exception(f'Ошибка проверки статуса задачи\n{response.status_code}\n{json.loads(response.text)}')
        
           
    @property
    def tasks_resaults(self):
        return [item for item in self.__tasks_resaults]
    
    def get_distinct_hosts(self):
        distinct_hosts = []
        for task_resault in self.__tasks_resaults:
            for keyword in task_resault['keywords']:
                for resault in keyword['serp'][:self.search_depth]:
                    host = resault['host'].replace('www.', '').lower()
                    if host not in distinct_hosts:
                        distinct_hosts.append(host)
        return distinct_hosts
    
    def get_query_resaults(self):
        self.final_data = []
        for task_resaults in self.__tasks_resaults:
            for keyword in task_resaults['keywords']:
                for resault in keyword['serp'][:self.search_depth]:
                    res={}
                    res['query'] = keyword['query']
                    res['position'] = resault['position']
                    res['url'] = resault['url']
                    res['host'] = resault['host']
                    res['title'] = resault['title']
                    self.final_data.append(res)
        
        return self.final_data

    def get_distinct_hosts_statistic(self, ):
        result = []
        for distinct_host in self.get_distinct_hosts():
            counter = 0
            for task_resaults in self.tasks_resaults:
                for keyword in task_resaults['keywords']:
                    for resault in keyword['serp'][:self.search_depth]:
                        if distinct_host.lower() in resault['host'].lower():
                            counter +=1
                            break;
            result.append(
                (distinct_host.lower(), round((counter / self.keywords_count)*100, 2))
            )
        return result
    
    def get_distinct_hosts_statistic_top10(self, ):
        result = []
        for distinct_host in self.get_distinct_hosts():
            counter = 0
            for task_resaults in self.tasks_resaults:
                for keyword in task_resaults['keywords']:
                    for resault in keyword['serp'][:10]:
                        if distinct_host.lower() in resault['host'].lower():
                            counter +=1
                            break;
            result.append(
                (distinct_host.lower(), round((counter / self.keywords_count)*100, 2))
            )
        return result

    def get_task_resault(self, taskId):
        response = self.session.get(self.__BASE_API_URL+f'/task/result/{taskId}', params = {'key':self.__API_KEY})
        if response.status_code == 200:
            result = json.loads(response.text)
            self.__tasks_resaults.append(result)
            return result
        else:
            raise ConnectionError(f'Не удалось получить результаты {taskId}')
          
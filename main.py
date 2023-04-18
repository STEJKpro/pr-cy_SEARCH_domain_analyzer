from time import sleep
from analyzer_service import SearchAnalyzer, DomainAnalyzer, SearchRepeater
import os
from pathlib import Path 
import pandas
import json
from datetime import datetime
import pickle

def case_1():
    search_engine = None
    search_type = None
    search_depth = None
    query_file_path = None

    while not query_file_path or not Path(query_file_path).is_file():
        query_file_path = str(input('Вставте путь к файлу c запросами для анализа (xlsx): \n')).strip('"')
        if not Path(query_file_path).is_file() or not query_file_path.endswith('.xlsx'):
            print('Не верное расширение файла или путь')


    # query_file_path = 'C:\\Users\\stejk\\Documents\\yandex_parsing\\project_v2\\test_query_set.xlsx'

    while search_engine  not in (1,2):
        search_engine = int(input("Выберети поисковую систему:\n1 - Google\n2 - Yandex\nВаш выбор: "))
    search_engine = 'google' if search_engine == 1 else 'yandex'

    while search_type not in (1,2):
        search_type = int(input("Тип поисковой выдачи:\n1 - мобильная выдача\n2 - ПК\nВаш выбор: "))
    search_type = 'mobile' if search_type == 1 else 'desktop'

    while search_depth not in (10,20,30):
        search_depth = int(input("Глубина топа по запросу(10/20/30):\nВаш выбор: "))


    print(
        f"Поисковая система: {search_engine}\n"\
        f"Тип поисковой выдачи: {'mobile' if search_type == 1 else 'desktop'}\n"\
        f"Глубина анализа: {search_depth}\n"
        f"Путь к файлу: {query_file_path}"
            )
    

    analyzer = SearchAnalyzer(
                    search_engine=search_engine, 
                    search_type=search_type, 
                    search_depth=search_depth, 
                    file_path=query_file_path
                    )
    
    print(f'Баланс составляет: {analyzer.get_balance()}')
    keywords_list = analyzer.get_keywords_from_file()

    with open('tasks_id.txt', 'w') as file:
        tasks_id = []
        for keywords in keywords_list:
            task_id = analyzer.creat_task(keywords)
            file.write(task_id+'\n')
            tasks_id.append(task_id)

    with open('keywords_count.txt', 'w') as file:
        file.write(str(analyzer.keywords_count))


    for task_id in tasks_id:
        while not analyzer.task_status_check(task_id):
            print(f"{datetime.now()} ### Задача с id: {task_id}, пока что не выполнена, до следующей проверки ожидайте 2 минуты")
            sleep(2*60) 
        analyzer.get_task_resault(task_id)

    df = pandas.DataFrame.from_records(analyzer.get_query_resaults())
    print (df)
    with open ('res.json', 'w', encoding='utf-8') as f:
        json.dump(analyzer.tasks_resaults, f, indent=4, ensure_ascii=False)
    

    df2 = pandas.DataFrame.from_records(analyzer.get_distinct_hosts_statistic())
    df['уникальные хосты'] = df2.iloc[:, 0]
    df['%'] = df2.iloc[:, 1]

    df2 = pandas.DataFrame.from_records(analyzer.get_distinct_hosts_statistic_top10())
    df['уникальные_в_топ10'] = df2.iloc[:, 0]
    df['%_топ10'] = df2.iloc[:, 1]

    desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
    file_path = f'{desktop}/{datetime.now().strftime("%d-%m-%Y__%H-%M")}.xlsx'
    df.to_excel(file_path, index=False)
    os.remove('keywords_count.txt')
    os.remove('tasks_id.txt')

    print('Результат выполнения в файле: ', file_path)
    print('\n#####################\n')
    

def case_2():
    domain_file_path = None

    while not domain_file_path or not Path(domain_file_path).is_file():
        domain_file_path = str(input('Вставте путь к файлу c запросами для анализа (xlsx): \n')).strip('"')
        if not Path(domain_file_path).is_file() or not domain_file_path.endswith('.xlsx'):
            print('Не верное расширение файла или путь')

    domain_analyzer = DomainAnalyzer(file_path=domain_file_path)
    domain_analyzer.get_domains_from_file()
    recheck_domains_list =[]


    for domain in domain_analyzer.domains:
        print (domain)
        status, comment = domain_analyzer.check_domain_status(domain)
        if not status:
            if comment == 'overdue' or 'new':
                domain_analyzer.update_domain_analysis(domain)
                recheck_domains_list.append(domain)
            if comment == 'updating' or comment == 'new':
                recheck_domains_list.append(domain)
        else:
            if not domain_analyzer.get_domain_analysis(domain):
                recheck_domains_list.append(domain)

    recheck_counts = {}
    while recheck_domains_list:
        for domain in recheck_domains_list:
            if not recheck_counts.get(domain):
                recheck_counts[domain] = 0

            recheck_counts[domain] += 1

            if recheck_counts.get(domain) <= 2:
                print('Ждем минуту прежде чем проверить домен повторно....: ', domain)
                sleep (60)
                status, comment = domain_analyzer.check_domain_status(domain)

                if not status:
                    if comment == 'overdue':
                        domain_analyzer.update_domain_analysis(domain)
                else:
                    if not domain_analyzer.get_domain_analysis(domain):
                        print('Привышен лимит запросов. Спим 1 час и возобновляем работу')
                        sleep(60*60)
                    else: recheck_domains_list.remove(domain)
            
            else:
                print(f'Домен {domain} не удалось обработать')
                recheck_domains_list.remove(domain)


    desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
    file_path = f'{desktop}\hosts_{datetime.now().strftime("%d-%m-%Y__%H-%M")}.xlsx'

    pandas.DataFrame.from_records(domain_analyzer.get_final_records()).to_excel(file_path, index=False, columns=['host',
'yandexIndex',
'Длина визита, сек',
'ip',
'Кол-во внешних ссылок по Мегаиндекс',
'ПК pageSpeed оценка',
'ПК pageSpeed в секундах',
'Мобильный pageSpeed оценка',
'Мобильный pageSpeed в секундах',
'Просмотры в мес',
'Посетители в мес',
'sitemap',
'Трафик по странам',
'Ист. траф. Прямые заходы',
'Ист. траф. Почтовые рассылки',
'Ист. траф. Ссылки на сайтах',
'Ист. траф. Поисковые системы',
'Ист. траф. Соц сети',
'Дата создания сайта',
'Сайт на https?',
'Есть Турбо?',
'Отзывов в Яндекс',
'Внешних ссылок на главную',
'Наш партнер?',
'Ответственный менеджер',
'Тип партнера',
])
    print('Результат выполнения в файле: ', file_path)

if __name__ == '__main__':
    try:
        with open('tasks_id.txt', 'r+') as file:
            tasks_id = file.readlines()
            tasks_id = [i.strip('\n') for i in tasks_id]
        with open('keywords_count.txt', 'r+') as file:
            keywords_count = int(file.read())

        print('Найдена незаконченная задач с id: ', tasks_id)

        if input('Чтобы возобновить данную задачу введите 0 (ноль)\nДля пропуска введите любое значение: ') == '0':
            search_depth = None
            while search_depth not in (10,20,30):
                search_depth = int(input("Глубина топа по запросу(10/20/30):\nВаш выбор: "))
            
            analyzer_1 = SearchRepeater(search_depth = search_depth, keywords_count = keywords_count)
            for task_id in tasks_id:
                while not analyzer_1.task_status_check(task_id):
                    print(f"{datetime.now()} ### Задача с id: {task_id}, пока что не выполнена, до следующей проверки ожидайте 2 минуты")
                    sleep(2*60) 
                analyzer_1.get_task_resault(task_id)

            df = pandas.DataFrame.from_records(analyzer_1.get_query_resaults())

            with open ('res.json', 'w', encoding='utf-8') as f:
                json.dump(analyzer_1.tasks_resaults, f, indent=4, ensure_ascii=False)
            

            df2 = pandas.DataFrame.from_records(analyzer_1.get_distinct_hosts_statistic())
            df['уникальные хосты'] = df2.iloc[:, 0]
            df['%'] = df2.iloc[:, 1]

            df2 = pandas.DataFrame.from_records(analyzer_1.get_distinct_hosts_statistic_top10())
            df['уникальные_в_топ10'] = df2.iloc[:, 0]
            df['%_топ10'] = df2.iloc[:, 1]

            desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
            file_path = f'{desktop}/{datetime.now().strftime("%d-%m-%Y__%H-%M")}.xlsx'
            df.to_excel(file_path, index=False)
            os.remove('keywords_count.txt')
            os.remove('tasks_id.txt')

            print('Результат выполнения в файле: ', file_path)
            print('\n#####################\n')
    except FileNotFoundError:
            pass
    select = None
    while select  not in (1,2):
        select = int(input ('Что будем делать?\n1 - Работа с поисковыми запросами\n2 - Работа с доменами\nВаш выбор: '))
    
    if select == 1: case_1()
    if select == 2: case_2()

########################################################

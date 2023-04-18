from time import sleep
from analyzer_service import SearchAnalyzer, DomainAnalyzer
import os
from pathlib import Path 
import pandas
import json
from datetime import datetime    
import time

domain_analyzer = DomainAnalyzer(file_path="C:\\Users\\stejk\\Documents\\yandex_parsing\\project_v2\\test domain set.xlsx")
domain_analyzer.get_domains_from_file()
recheck_domains_list =[]


print (domain_analyzer.check_domain_status('dev.by'))
for domain in domain_analyzer.domains:
    status, comment = domain_analyzer.check_domain_status(domain)
    if not status:
        if comment == 'overdue':
            domain_analyzer.update_domain_analysis(domain)
            recheck_domains_list.append(domain)
        if comment == 'updating':
            recheck_domains_list.append(domain)
    else:
        domain_analyzer.get_domain_analysis(domain)

recheck_counts = {}
while recheck_domains_list:
    for domain in recheck_domains_list:
        if not recheck_counts.get(domain):
            recheck_counts[domain] = 0

        recheck_counts[domain] += 1

        if recheck_counts.get(domain) <= 2:
            print('Ждем минуту прежде чем проверить домен повторно....: ', domain)
            time.sleep (60)
            status, comment = domain_analyzer.check_domain_status(domain)

            if not status:
                if comment == 'overdue':
                    domain_analyzer.update_domain_analysis(domain)
                    recheck_domains_list.append(domain)
                if comment == 'updating':
                    recheck_domains_list.append(domain)
            else:
                domain_analyzer.get_domain_analysis(domain)
                recheck_domains_list.remove(domain)
        
        else:
            print(f'Домен {domain} не удалось обработать')
            recheck_domains_list.remove(domain)

pandas.DataFrame.from_records(domain_analyzer.get_final_records()).to_excel('domain_resaults.xlsx')
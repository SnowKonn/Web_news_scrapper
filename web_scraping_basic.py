import time
import pandas as pd
import numpy as np
import datetime as dt
from Packages.Web_scrapper import *

if __name__ == "__main__":
    local_db_file_path = './Data/db_instance.db'
    past_date_length = 2

    scrapper_local_db = Scrap_DB(local_db_file_path)
    scrapper_local_db.create_basic_table()
    local_db_table_name = 'news_titles'

    root_url = "https://finance.naver.com"
    target_root_page_url = 'https://finance.naver.com/news/mainnews.nhn'
    date_list = [(dt.datetime.now() - dt.timedelta(i)).strftime('%Y-%m-%d') for i in range(past_date_length)]

    news_title_scrapper = Webscrapper()

    tot_results_dict= {}

    for date_str in date_list:
        target_url = target_root_page_url + '?date=' + date_str

        news_title_scrapper.set_target_pages(target_url)

        result_soup = news_title_scrapper.req_html_to_soup()
        news_table = result_soup.find_all(id='contentarea_left')
        article_heads = news_table[0].find_all(class_='articleSubject')

        results = news_title_scrapper.clear_string(article_heads)
        url_result = news_title_scrapper.get_news_url(article_heads, root_url)
        tot_results_dict[date_str] = {'headline': results, 'news_link': url_result}
        time.sleep(1)
    result_tidy_format_list = []

    for date_key in tot_results_dict:
        tot_results_dict[date_key] = pd.DataFrame(tot_results_dict[date_key])
        tot_results_dict[date_key]['days'] = date_key
        result_tidy_format_list.append(tot_results_dict[date_key])

    fin_result_pd = pd.concat(result_tidy_format_list)
    db_columns = fin_result_pd.columns.tolist()
    fin_result_db_form = np.array(fin_result_pd).tolist()

    scrapper_local_db.insert_database_multi_rows(local_db_table_name,db_columns, fin_result_db_form)

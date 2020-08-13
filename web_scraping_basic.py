import time
import pandas as pd
import numpy as np
import datetime as dt
from Packages.Web_scrapper import *

def numbering_the_titles(title_list):
    for i in range(len(title_list)):
        title_list[i] = str(i+1) + '. ' + title_list[i]

    return title_list

if __name__ == "__main__":

    local_db_file_path = './Data/db_instance.db'
    past_date_length = 5

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
        time.sleep(0.2)
    result_tidy_format_list = []

    for date_key in tot_results_dict:
        tot_results_dict[date_key] = pd.DataFrame(tot_results_dict[date_key])
        tot_results_dict[date_key]['days'] = date_key
        result_tidy_format_list.append(tot_results_dict[date_key])

    fin_result_pd = pd.concat(result_tidy_format_list)
    db_columns = fin_result_pd.columns.tolist()
    fin_result_db_form = np.array(fin_result_pd).tolist()

    scrapper_local_db.insert_non_exist_row_database_multi_rows(local_db_table_name, db_columns, fin_result_db_form)

    # Send massage
    noti_instance = LineNotifier()
    news_columns = ['days', 'headline', 'news_link']
    today_str = dt.datetime.now().strftime('%Y-%m-%d')

    select_today_headline = "SELECT %s FROM news_titles where days = '%s' and notify_check IS NULL" % (', '.join(news_columns), today_str)
    results = scrapper_local_db.select_db(select_today_headline)

    results_pd = pd.DataFrame(results, columns=news_columns)

    print("Financial: \n", "\n".join(results_pd['headline']), sep='')
    title_list = results_pd['headline'].to_list()
    url_list = results_pd['news_link'].to_list()

    num_title_list = numbering_the_titles(title_list)
    num_url_list = numbering_the_titles(url_list)

    chunk_size = 5

    progress_state = False
    for i in range(int((len(title_list)/chunk_size-1))+1):
        if (i+1) * chunk_size < len(title_list):
            temp_title_list = num_title_list[(i*chunk_size):((i+1) * chunk_size)]
        else:
            temp_title_list = num_title_list[(i * chunk_size):]
        noti_instance.post_message('Financial \n' + '\n\n'.join(temp_title_list))


    progress_state = False
    for i in range(int((len(title_list)/chunk_size-1))+1):
        if (i + 1) * chunk_size < len(title_list):
            temp_url_list = num_url_list[(i*chunk_size):((i+1) * chunk_size)]
        else:
            temp_url_list = num_url_list[(i * chunk_size):]
        noti_instance.post_message('Financial \n' + '\n\n'.join(temp_url_list))

    results_pd['notify_check'] = str(1)

    update_list = results_pd.columns

    update_check_array = np.array(results_pd.loc[results_pd['days'] >= today_str])
    scrapper_local_db.replace_database_multi_rows('news_titles', update_list, update_check_array)


from bs4 import BeautifulSoup
import re
from Packages.Notify_method import *
from Packages.Data_Base_Method import LocalDBMethods

class Webscrapper:

    def __init__(self):
        self.results = None
        self.noti = LineNotifier()

    def set_target_pages(self, url):
        self.target_url = url

    def req_html_to_soup(self):

        req = requests.get(self.target_url)
        soup = BeautifulSoup(req.text, features='lxml')

        return soup

    def get_news_url(self, soup_list, root_url):
        url_list = []

        for i in range(len(soup_list)):
            soup_obj_i = soup_list[i]
            a_tag = soup_obj_i.find('a')
            url_result = a_tag['href']
            url_list.append(root_url + url_result)

        return url_list

    def clear_string(self, soup_list):
        result_list = []

        for i in range(len(soup_list)):
            # Remove redundant strings by using regular expression
            temp_text = re.sub(r"\n*", "", soup_list[i].text)
            temp_text = re.sub(r"^[ ]", "", temp_text)
            temp_text = re.sub(r"^\[\w+\]|^\[\w+[ ]\w+\]", "", temp_text)
            temp_text = re.sub(r"^[ ]", "", temp_text)
            temp_text = re.sub(r"\[\w+\]$|\[\w+[ ]\w+\]$", "", temp_text)
            # temp_text = str(i + 1) + '. ' + temp_text
            result_list.append(temp_text)

        return result_list

    def post_massages(self, msg):
        self.noti.post_message(msg)


class Scrap_DB(LocalDBMethods):

    def __init__(self, dir_db):
        super().__init__(dir_db)

    def create_basic_table(self):

        create_table_sql = """CREATE TABLE IF NOT EXISTS news_titles(
                               notify_check integer,
                               days text,
                               headline text,
                               news_link text PRIMARY KEY,
                               etc text)
                               """

        self.excecute_sql_query(create_table_sql)

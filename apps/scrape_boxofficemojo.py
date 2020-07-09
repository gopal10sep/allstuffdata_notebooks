import requests
import time
import re
from datetime import datetime

from bs4 import BeautifulSoup
from sqlalchemy.sql.expression import func

from macro import application
from macro.dbs import oracle as oracle_db
from macro.db_models import sql_base
from macro.db_models.oracle import BOMDataPoint as BOMDP

proxies = {
    "http": "proxy.jpmchase.net:8443",
    "https": "proxy.jpmchase.net:8443",
}


def get_soup(link):
    response = requests.get(link, proxies=proxies)
    soup = BeautifulSoup(response.content, "html.parser")
    return soup


class App(application.Application):
    def app_name(self):
        return 'scrape_boxofficemojo'

    def __init__(self, args):
        super().__init__()
        self.args = args
        self.sql = oracle_db.Session()
        sql_base.SqlBase.metadata.create_all(oracle_db.engine)
        self.n_added = 0

    def run_application(self):
        year_list = self.make_year_list()
        complete_list = self.get_link_list()
        self.insert_data(complete_list, year_list)
        self.sql.commit()
        self.info('Run complete: {0} data points added'.format(self.n_added))

    def determine_start_year(self):
        start_year = self.sql.query(func.max(BOMDP.year)).one_or_none()
        start_year = start_year[0] if start_year[0] else 1973
        self.info('Starting with {0}...'.format(start_year))
        return start_year

    def make_year_list(self):
        syear = self.determine_start_year()
        eyear = datetime.now().year
        year_list = []
        for i in range(syear, eyear + 1):
            year_list.append(i)
        return year_list

    def get_link_list(self):
        link = "https://www.boxofficemojo.com/intl/?ref_=bo_nb_ydw_tab"
        soup = get_soup(link)
        links = soup.findAll("form", {"class": "a-spacing-none"})[0]
        complete_list = []
        for item in links.findAll("option"):
            row = [item.text, "https://www.boxofficemojo.com" + item["value"]]
            complete_list.append(row)
        complete_list.append(['United States', 'https://www.boxofficemojo.com/weekly/by-year/2019/'])
        return complete_list

    def insert_data(self, complete_list, year_list):
        try:
            for i in range(len(complete_list)):
                row = complete_list[i]
                self.info("Working on region " + str(row[0]))
                match = re.search('\d{4}', row[1])
                region = row[0].replace(" ", "").replace("&", "And").replace("/", "_").replace("(", "_").replace(")",
                                                                                                                 "")
                for year in year_list:
                    link = row[1].replace(match[0], str(year))
                    try:
                        content = get_soup(link)
                        try:
                            req = content.findAll("table", {
                                "class": "a-bordered a-horizontal-stripes a-size-base a-span12 mojo-body-table mojo-table-annotated"})[
                                0]
                            for item in req.findAll("tr")[1:]:
                                d = {'year': int(year), 'region': region}
                                td_list = item.findAll("td")
                                d['date'] = td_list[0].text
                                try:
                                    d['top10Gross'] = int(td_list[1].text.replace(",", "").replace("$", ""))
                                except:
                                    d['top10Gross'] = 0
                                try:
                                    d['percentLW_T10G'] = float(td_list[2].text.replace("%", ""))
                                except:
                                    d['percentLW_T10G'] = 0
                                try:
                                    d['overallGross'] = int(td_list[3].text.replace(",", "").replace("$", ""))
                                except:
                                    d['overallGross'] = 0
                                try:
                                    d['percentLW_OG'] = float(td_list[4].text.replace("%", ""))
                                except:
                                    d['percentLW_OG'] = 0
                                try:
                                    d['releases'] = int(td_list[5].text)
                                except:
                                    d['releases'] = 0
                                d['no1_Release'] = td_list[6].text
                                try:
                                    d['week'] = int(td_list[10].text)
                                except:
                                    d['week'] = td_list[10].text
                                chk = self.sql.query(BOMDP).filter(
                                    BOMDP.year == d['year']).filter(
                                    BOMDP.region == d['region']).filter(
                                    BOMDP.week == d['week']).all()
                                if chk:
                                    continue
                                self.sql.add(BOMDP(**d))
                                self.n_added += 1
                            self.sql.commit()
                        except Exception as e:
                            self.info(e)
                            pass
                    except Exception as e:
                        self.info(e)
                        pass
        except Exception as e:
            self.info(e)
            pass

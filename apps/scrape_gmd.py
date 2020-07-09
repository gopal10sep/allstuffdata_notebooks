import requests
import io
from datetime import datetime, timedelta

from sqlalchemy.sql.expression import func
import pandas as pd

from macro import application
from macro.dbs import oracle as oracle_db
from macro.db_models import sql_base
from macro.db_models.oracle import GMDataPoint as GMDP

proxies = {
    "http": "proxy.jpmchase.net:8443",
    "https": "proxy.jpmchase.net:8443",
}


class App(application.Application):
    def app_name(self):
        return 'scrape_gmd'

    def __init__(self, args):
        super().__init__()
        self.args = args
        self.sql = oracle_db.Session()
        sql_base.SqlBase.metadata.create_all(oracle_db.engine)

    def run_application(self):
        start_date = self.determine_start_date()

        link = "http://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"
        s = requests.get(link, proxies=proxies).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df.columns = ['country_region_code', 'country_region', 'sub_region_1', 'sub_region_2', 'date',
                      'retail_and_recreation', 'grocery_and_pharmacy', 'parks', 'transit_stations', 'workplaces',
                      'residential']
        df.date = pd.to_datetime(df.date)
        df = df[df.date >= start_date].reset_index(drop=True)
        df = df.sort_values(by=['date', 'country_region_code', 'country_region', 'sub_region_1', 'sub_region_2'])
        df = df.astype(object).where(pd.notnull(df), None)
        df['sub_region_1'] = df['sub_region_1'].str.encode('ascii', 'ignore').str.decode('ascii')
        df['sub_region_2'] = df['sub_region_2'].str.encode('ascii', 'ignore').str.decode('ascii')
        df = df.reset_index(drop=True)

        n_added = 0
        for i, row in df.iterrows():
            if not i % 5000:
                self.info(i)
                self.sql.commit()
            chk = self.sql.query(GMDP).filter(
                GMDP.country_region == row['country_region']).filter(
                GMDP.sub_region_1 == row['sub_region_1']).filter(
                GMDP.sub_region_2 == row['sub_region_2']).filter(
                GMDP.date == row['date']).all()
            if chk:
                continue
            self.sql.add(GMDP(**row.to_dict()))
            n_added += 1
        self.sql.commit()
        self.info('{0} data points added.'.format(n_added))

    def determine_start_date(self):
        start_date = self.sql.query(func.max(GMDP.date)).one_or_none()
        start_date = start_date[0] - timedelta(days=3) if start_date[0] else datetime(2020, 1, 1)
        self.info('Starting with {0}...'.format(start_date))
        return start_date

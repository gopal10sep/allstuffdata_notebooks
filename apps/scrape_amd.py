import os
from datetime import datetime, timedelta

from sqlalchemy.sql.expression import func
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time

import macro
from macro import application
from macro.dbs import oracle as oracle_db
from macro.db_models import sql_base
from macro.db_models.oracle import AMDataPoint as AMDP

DATADIR = os.path.join(macro.ROOT, 'data')


class App(application.Application):
    """
    Requires that you have chromedriver.exe in the etc directory.
    First, see what version of chrome you have: chrome -> about
    Then find the corresponding driver here: https://chromedriver.chromium.org/downloads
    """

    def app_name(self):
        return 'scrape_amd'

    def __init__(self, args):
        super().__init__()
        self.sql = oracle_db.Session()
        sql_base.SqlBase.metadata.create_all(oracle_db.engine)

    def run_application(self):
        try:
            for item in os.listdir(DATADIR):
                if "#applemobilitytrends" in item:
                    os.remove(DATADIR + "\\" + item)
        except:
            pass
        self.driver = self._initialize_driver()
        self.driver.get("https://www.apple.com/covid19/mobility")
        time.sleep(3)
        elem = WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="download-card"]/div[2]/ui-button')))
        elem.click()
        self.info("Download Button Clicked.")
        time.sleep(5)
        self.driver.close()
        self.sync_csv_data_with_database()
        self.info('Run complete.')

    def sync_csv_data_with_database(self):
        self.info('Syncing csv with database...')
        for item in os.listdir(DATADIR):
            if "applemobilitytrends" in item:
                expected_filename = os.path.join(DATADIR, item)
        df = pd.read_csv(expected_filename)
        col_range = list(df.columns)[3:]
        df = pd.melt(df, id_vars=['geo_type', 'region', 'transportation_type'], value_vars=col_range)
        df.columns = ['geo_type', 'region', 'transportation_type', 'date', 'value']
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = df['value'].astype(float)
        df = df[['date', 'geo_type', 'region', 'transportation_type', 'value']]
        df = df.sort_values(by=['date', 'geo_type', 'region', 'transportation_type'])

        start_date = self.determine_start_date()
        df = df[df.date >= start_date].reset_index(drop=True)

        n_added = 0
        for i, row in df.iterrows():
            if not i % 5000:
                self.info(i)
            chk = self.sql.query(AMDP).filter(
                AMDP.geo_type == row['geo_type']).filter(
                AMDP.region == row['region']).filter(
                AMDP.transportation_type == row['transportation_type']).filter(
                AMDP.date == row['date']).all()
            if chk:
                continue
            self.sql.add(AMDP(**row.to_dict()))
            n_added += 1
        self.sql.commit()
        self.info('{0} records added.'.format(n_added))

    def _initialize_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')

        prefs = {
            "profile.default_content_settings.popups": 0,
            "download.prompt_for_download": False,
            "download.default_directory": DATADIR,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            'extensions.disabled': True,
        }
        options.add_experimental_option("prefs", prefs)
        driver_loc = os.path.join(macro.ROOT, 'etc', "chromedriver.exe")
        while True:
            try:
                driver = webdriver.Chrome(driver_loc, chrome_options=options)
                break
            except:
                pass
        return driver

    def determine_start_date(self):
        start_date = self.sql.query(func.max(AMDP.date)).one_or_none()
        start_date = start_date[0] - timedelta(days=3) if start_date[0] else datetime(2020, 1, 1)
        self.info('Starting with {0}...'.format(start_date))
        return start_date

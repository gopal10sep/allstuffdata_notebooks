import time
import urllib
import argparse
from datetime import datetime
import pytz

import requests
import numpy as np
import sqlalchemy
from sqlalchemy.sql.expression import func
from json.decoder import JSONDecodeError

from macro import application
from macro import apptime
from macro.dbs import oracle as oracle_db
from macro.db_models import sql_base
from macro.db_models.oracle import TrafficStatLocation, TrafficStat
from macro.datasets.traffic_stats_dataset import slug_to_pop

LOCATIONS = (  # (slug, path, nice name)

        # USA
        ('new-york', 'USA/Circle/new-york', 'New York, US', int(8.39e6), int(8107916)),
        ('new-orleans', 'USA/Circle/new-orleans', 'New Orleans, US', int(3.91e5), int(459336)),
        ('denver', 'USA/Circle/denver', 'Denver, US', int(6.19e5), int(555981)),
        ('detroit', 'USA/Circle/detroit', 'Detroit, US', int(6.7e5), int(884941)),
        ('virginia-beach', 'USA/Circle/virginia-beach', 'Virginia Beach, US', int(449386)),
        ('el-paso', 'USA/Circle/el-paso', 'El Paso, US', int(597181)),
        ('grand-rapids', 'USA/Circle/grand-rapids', 'Grand Rapids, US', int(193852)),
        ('louisville', 'USA/Circle/louisville', 'Louisville, US', int(243639)),
        ('buffalo', 'USA/Circle/buffalo', 'Buffalo, US', int(279557)),
        ('memphis', 'USA/Circle/memphis', 'Memphis, US', int(641608)),
        ('indianapolis', 'USA/Circle/indianapolis', 'Indianapolis, US', int(773283)),
        ('omaha-council-bluffs', 'USA/Circle/omaha-council-bluffs', 'Omaha-Council Bluffs, US', int(468262)),
        ('oklahoma-city', 'USA/Circle/oklahoma-city', 'Oklahoma City, US', int(532950)),
        ('columbia', 'USA/Circle/columbia', 'columbia, US', int(117722)),
        ('milwaukee', 'USA/Circle/milwaukee', 'Milwaukee, US', int(579180)),
        ('cleveland', 'USA/Circle/cleveland', 'Cleveland, US', int(449514)),
        ('kansas-city', 'USA/Circle/kansas-city', 'Kansas City, US', int(442028)),
        ('richmond', 'USA/Circle/richmond', 'Richmond, US', int(190886)),
        ('dallas-fort-worth', 'USA/Circle/dallas-fort-worth', 'Dallas-Fort Worth, US', int(7690420)),
        ('chicago', 'USA/Circle/chicago', 'Chicago, US', int(2841952)),
        ('san-francisco', 'USA/Circle/san-francisco', 'San Francisco, US', int(732072)),
        ('san-jose', 'USA/Circle/san-jose', 'San Jose, US', int(897460)),
        ('washington', 'USA/Circle/washington', 'Washington, US', int(552433)),
        ('seattle', 'USA/Circle/seattle', 'Seattle, US', int(569369)),
        ('los-angeles', 'USA/Circle/los-angeles', 'Los Angeles, US', int(3877129)),
        ('atlanta', 'USA/Circle/atlanta', 'Atlanta, US', int(422908)),
        ('phoenix', 'USA/Circle/phoenix', 'Phoenix, US', int(1428509)),
        ('tucson', 'USA/Circle/tucson', 'Tucson, US', int(518907)),
        ('baltimore', 'USA/Circle/baltimore', 'Baltimore, US', int(610892)),
        ('boston', 'USA/Circle/boston', 'Boston, US', int(571281)),
        ('columbus', 'USA/Circle/columbus', 'Columbus, US', int(736836)),
        ('cincinnati', 'USA/Circle/cincinnati', 'Cincinnati, US', int(306382)),
        ('jacksonville', 'USA/Circle/jacksonville', 'Jacksonville, US', int(797557)),
        ('hartford', 'USA/Circle/hartford', 'Hartford, US', int(124019)),
        ('raleigh', 'USA/Circle/raleigh', 'Raleigh, US', int(338759)),
        ('salt-lake-city', 'USA/Circle/salt-lake-city', 'Salt Lake City, US', int(178026)),
        ('mcallen', 'USA/Circle/mcallen', 'McAllen, US', int(143433)),
        ('oxnard-thousand-oaks-ventura', 'USA/Circle/oxnard-thousand-oaks-ventura', 'Oxnard-Thousand Oaks-Ventura, US', int(111128)),
        ('providence', 'USA/Circle/providence', 'Providence, US', int(177595)),
        ('charlotte', 'USA/Circle/charlotte', 'Charlotte, US', int(598351)),
        ('pittsburgh', 'USA/Circle/pittsburgh', 'Pittsburgh, US', int(319494)),
        ('austin', 'USA/Circle/austin', 'Austin, US', int(678368)),
        ('sacramento', 'USA/Circle/sacramento', 'Sacramento, US', int(467898)),
        ('portland', 'USA/Circle/portland', 'Portland, US', int(540513)),
        ('honolulu', 'USA/Circle/honolulu', 'Honolulu, US', int(384241)),
        ('minneapolis', 'USA/Circle/minneapolis', 'Minneapolis, US', int(4.25e5), int(367773)),
        ('st-louis', 'USA/Circle/st-louis', 'Saint Louis, US', int(320916)),
        ('houston', 'USA/Circle/houston', 'Houston, US', int(2027712)),
        ('tampa', 'USA/Circle/tampa', 'Tampa, US', int(324465)),
        ('miami', 'USA/Circle/miami', 'Miami, US', int(382894)),
        ('nashville', 'USA/Circle/nashville', 'Nashville, US', int(530852)),
        ('riverside', 'USA/Circle/riverside', 'Riverside, US', int(297554)),
        ('las-vegas', 'USA/Circle/las-vegas', 'Las Vegas, US', int(540111)),
        ('orlando', 'USA/Circle/orlando', 'Orlando, US', int(207970)),
        ('philadelphia', 'USA/Circle/philadelphia', 'Philadelphia, US', int(1453268)),
        ('san-antonio', 'USA/Circle/san-antonio', 'San Antonio, US', int(1256810)),
        ('san-diego', 'USA/Circle/san-diego', 'San Diego, US', int(1287050)),

        # CHINA
        ('wuhan', 'CHN/Circle/wuhan', 'Wuhan, CN', int(4184206)),
        ('wuxi', 'CHN/Circle/wuxi', 'Wuxi, CN', int(1108647)),
        ('chongqing', 'CHN/Circle/chongqing', 'Chongqing, CN', int(3967028)),
        ('beijing', 'CHN/Circle/beijing', 'Beijing, CN', int(21.54e6)),
        ('shanghai', 'CHN/Circle/shanghai', 'Shanghai, CN', int(14608512)),
        ('shijiazhuang', 'CHN/Circle/shijiazhuang', 'Shijiazhuang, CN', int(11.03e6)),
        ('tianjin', 'CHN/Circle/tianjin', 'Tianjin, CN', int(3766207)),
        ('guangzhou', 'CHN/Circle/guangzhou', 'Guangzhou, CN', int(3152825)),
        ('ningbo', 'CHN/Circle/ningbo', 'Ningbo, CN', int(719867)),
        ('shenzhen', 'CHN/Circle/shenzhen', 'Shenzhen, CN', int(1002592)),
        ('zhuhai', 'CHN/Circle/zhuhai', 'Zhuhai, CN', int(501199)),
        ('changsha', 'CHN/Circle/changsha', 'Changsha, CN', int(7.432e6)),
        ('chengdu', 'CHN/Circle/chengdu', 'Chengdu, CN', int(3950437)),
        ('changchun', 'CHN/Circle/changchun', 'Changchun, CN', int(2537421)),
        ('xiamen', 'CHN/Circle/xiamen', 'Xiamen, CN', int(578337)),
        ('nanjing', 'CHN/Circle/nanjing', 'Nanjing, CN', int(3087010)),
        ('hangzhou', 'CHN/Circle/hangzhou', 'Hangzhou, CN', int(1878129)),
        ('fuzhou', 'CHN/Circle/fuzhou', 'Fuzhou, CN', int(1179720)),
        ('shenyang', 'CHN/Circle/shenyang', 'Shenyang, CN', int(3512192)),
        ('quanzhou', 'CHN/Circle/quanzhou', 'Quanzhou, CN', int(184143)),
        ('dongguan', 'CHN/Circle/dongguan', 'Dongguan, CN', int(8.26e6)),
        ('suzhou', 'CHN/Circle/suzhou', 'Suzhou, CN', int(1343091)),

        # JAPAN
        ('tokyo', 'JPN/Circle/tokyo', 'Tokyo, JP', int(31480498)),
        ('kyoto', 'JPN/Circle/kyoto', 'Kyoto, JP', int(1.475e6)),
        ('osaka', 'JPN/Circle/osaka', 'Osaka, JP', int(57170)),
        ('sapporo', 'JPN/Circle/sapporo', 'Sapporo, JP', int(1.952e6)),
        ('yokohama', 'JPN/Circle/yokohama', 'Yokohama, JP', int(3.725e6)),
        ('nagoya', 'JPN/Circle/nagoya', 'Nagoya, JP', int(2191291)),
        ('kobe', 'JPN/Circle/kobe', 'Kobe, JP', int(1528487)),

        # SOUTH KOREA - none?

        # TAIWAN
        ('taipei', 'TWN/Circle/taipei', 'Taipei, TW', int(2514794)),
        ('taichung', 'TWN/Circle/taichung', 'Taichung, TW', int(1083582)),
        ('kaohsiung', 'TWN/Circle/kaohsiung', 'Kaohsiung, TW', int(1512832)),
        ('tainan', 'TWN/Circle/tainan', 'Tainan, TW', int(734314)),
        ('taoyuan', 'TWN/Circle/taoyuan', 'Taoyuan, TW', int(375366)),

        # SINGAPORE
        ('singapore', 'SGP/Circle/singapore', 'Singapore, SG', int(3547809)),

        # HONG KONG
        ('hong-kong', 'HKG/Circle/hong-kong', 'Hong Kong, HK', int(7.451e6)),

        # Other Asia
        ('manila', 'PHL/Circle/manila', 'Manila, PH', int(10443877)),
        ('jakarta', 'IDN/Circle/jakarta', 'Jakarta, Indonesia', int(8540306)),
        ('bangkok', 'THA/Circle/bangkok', 'Bangkok, Thailand', int(5104475)),
        ('kuala-lumpur', 'MYS/Circle/kuala-lumpur', 'Kuala Lumpur, Malaysia', int(1453978)),
        ('auckland', 'NZL/Circle/auckland', 'Auckland, New Zealand', int(417910)),

        # INDIA
        ('mumbai', 'IND/Circle/mumbai', 'Mumbai, IN', int(18.41e6)),
        ('pune', 'IND/Circle/pune', 'Pune, IN', int(2935968)),
        ('bengaluru', 'IND/Circle/bangalore', 'Bangalore, IN', int(4931603)),
        ('new-delhi', 'IND/Circle/new-delhi', 'New Delhi, IN', int(10928270)),
        ('chennai', 'IND/Circle/chennai', 'Chennai, IN', int(7.088e6)),
        ('hyderabad', 'IND/Circle/hyderabad', 'Hyderabad, IN', int(3598199)),

        # MIDDLE EAST
        ('jeddah', 'SAU/Circle/jeddah', 'Jeddah, SA', int(3.431e6)),
        ('cairo', 'EGY/Circle/cairo', 'Cairo, EG', int(7734602)),
        ('abu-dhabi', 'ARE/Circle/abu-dhabi', 'Abu Dhabi, AE', int(603687)),
        ('dubai', 'ARE/Circle/dubai', 'Dubai, AE', int(1137376)),
        ('tel-aviv', 'ISR/Circle/tel-aviv', 'Tel Aviv, IS', int(370480)),
        ('jerusalem', 'ISR/Circle/jerusalem', 'Jerusalem, IS', int(714685)),
        ('kuwait-city', 'KWT/Circle/kuwait-city', 'Kuwait City, Kuwait', int(4.27e6)),
        ('riyadh', 'SAU/Circle/riyadh', 'Riyadh, SA', int(3469290)),

        # AUSTRALIA
        ('sydney', 'AUS/Circle/sydney', 'Sydney, AU', int(4394585)),
        ('melbourne', 'AUS/Circle/melbourne', 'Melbourne, AU', int(3730212)),
        ('brisbane', 'AUS/Circle/brisbane', 'Brisbane, AU', int(1843459)),
        ('adelaide', 'AUS/Circle/adelaide', 'Adelaide, AU', int(1074168)),
        ('perth', 'AUS/Circle/perth', 'Perth, AU', int(1446715)),

        # ITALY
        ('rome', 'ITA/Circle/rome', 'Rome, IT', int(2643736)),
        ('milan', 'ITA/Circle/milan', 'Milan, IT', int(1156903)),
        ('turin', 'ITA/Circle/turin', 'Turin, IT', int(846489)),
        ('naples', 'ITA/Circle/milan', 'Naples, IT', int(981284)),
        ('catania', 'ITA/Circle/catania', 'Catania, IT', int(307331)),
        ('pescara', 'ITA/Circle/pescara', 'Pescara, IT', int(118136)),
        ('bologna', 'ITA/Circle/bologna', 'Bologna, IT', int(367168)),
        ('prato', 'ITA/Circle/prato', 'Prato, IT', int(178915)),
        ('modena', 'ITA/Circle/modena', 'Modena, IT', int(180641)),
        ('florence', 'ITA/Circle/florence', 'Florence, IT', int(347194)),
        ('cagliari', 'ITA/Circle/cagliari', 'Cagliari, IT', int(157343)),
        ('taranto', 'ITA/Circle/taranto', 'Taranto, IT', int(191364)),
        ('parma', 'ITA/Circle/parma', 'Parma, IT', int(155693)),
        ('palermo', 'ITA/Circle/palermo', 'Palermo, IT', int(669582)),

        # UK
        ('london', 'GBR/Circle/london', 'London, UK', int(7421228)),
        ('manchester', 'GBR/Circle/manchester', 'Manchester, UK', int(395516)),
        ('birmingham', 'GBR/Circle/birmingham', 'Birmingham, UK', int(984336)),
        ('cardiff', 'GBR/Circle/cardiff', 'Cardiff, UK', int(302142)),
        ('liverpool', 'GBR/Circle/liverpool', 'Liverpool, UK', int(468946)),
        ('sheffield', 'GBR/Circle/sheffield', 'Sheffield, UK', int(447048)),
        ('leeds-bradford', 'GBR/Circle/leeds-bradford', 'Leeds-Bradford, UK', int(4.74e5)),
        ('glasgow', 'GBR/Circle/glasgow', 'Glasgow, UK', int(610271)),

        # CANADA
        ('toronto', 'CAN/Circle/toronto', 'Toronto, CN', int(4612187)),
        ('vancouver', 'CAN/Circle/vancouver', 'Vancouver, CN', int(1837970)),
        ('quebec', 'CAN/Circle/quebec', 'Quebec, CN', int(645623)),
        ('montreal', 'CAN/Circle/montreal', 'Montreal, CN', int(3268513)),
        ('ottawa', 'CAN/Circle/ottawa', 'Ottawa, CN', int(874433)),
        ('calgary', 'CAN/Circle/calgary', 'Calgary, CN', int(968475)),
        ('edmonton', 'CAN/Circle/edmonton', 'Edmonton, CN', int(822319)),

        # FRANCE
        ('paris', 'FRA/Circle/paris', 'Paris, FR', int(2110694)),
        ('lyon', 'FRA/Circle/lyon', 'Lyon, FR', int(463700)),
        ('marseille', 'FRA/Circle/marseille', 'Marseille, FR', int(792823)),
        ('bordeaux', 'FRA/Circle/bordeaux', 'Bordeaux, FR', int(219311)),
        ('toulouse', 'FRA/Circle/toulouse', 'Toulouse, FR', int(411145)),
        ('lille', 'FRA/Circle/lille', 'Lille, FR', int(189746)),

        # Other Europe
        ('brussels', 'BEL/Circle/brussels', 'Brussels, BE', int(1019022)),
        ('antwerp', 'BEL/Circle/antwerp', 'Antwerp, BE', int(459805)),
        ('stockholm', 'SWE/Circle/stockholm', 'Stockholm, SE', int(1253309)),
        ('oslo', 'NOR/Circle/oslo', 'Oslo, Norway', int(808690)),
        ('helsinki', 'FIN/Circle/helsinki', 'Helsinki, Finland', int(558457)),
        ('basel', 'CHE/Circle/basel', 'Basel, Switzerland', int(164474)),
        ('copenhagen', 'DNK/Circle/copenhagen', 'Copenhagen, DK', int(1089958)),
        ('barcelona', 'ESP/Circle/barcelona', 'Barcelona, SP', int(1570378)),
        ('seville', 'ESP/Circle/seville', 'Seville, SP', int(1.95e6)),
        ('valencia', 'ESP/Circle/valencia', 'Valencia, SP', int(769897)),
        ('madrid', 'ESP/Circle/madrid', 'Madrid, SP', int(3102644)),
        ('bilbao', 'ESP/Circle/bilbao', 'Bilbao, SP', int(349270)),
        ('amsterdam', 'NLD/Circle/amsterdam', 'Amsterdam, NL', int(745811)),
        ('the-hague', 'NLD/Circle/the-hague', 'The Hague, NL', int(476587)),
        ('rotterdam', 'NLD/Circle/rotterdam', 'Rotterdam, NL', int(603851)),
        ('lisbon', 'PRT/Circle/lisbon', 'Lisbon, PT', int(517798)),
        ('porto', 'PRT/Circle/porto', 'Porto, PT', int(249630)),
        ('athens', 'GRC/Circle/athens', 'Athens, GR', int(729139)),
        ('thessaloniki', 'GRC/Circle/thessaloniki', 'Thessaloniki, GR', int(354291)),
        ('vienna', 'AUT/Circle/vienna', 'Vienna, AT', int(1569315)),
        ('istanbul', 'TUR/Circle/istanbul', 'Istanbul, Turkey', int(9797536)),
        ('ankara', 'TUR/Circle/ankara', 'Ankara, Turkey', int(3519177)),
        ('konya', 'TUR/Circle/konya', 'Konya, Turkey', int(876004)),
        ('adana', 'TUR/Circle/adana', 'Adana, Turkey', int(1249680)),
        ('izmir', 'TUR/Circle/izmir', 'Izmir, Turkey', int(2501895)),
        ('bursa', 'TUR/Circle/bursa', 'Bursa, Turkey', int(1413485)),
        ('mersin', 'TUR/Circle/mersin', 'Mersin, Turkey', int(612540)),
        ('gaziantep', 'TUR/Circle/gaziantep', 'Gaziantep, Turkey', int(1066561)),
        ('kiev', 'UKR/Circle/kiev', 'Kyiv, Ukraine', int(2.884e6)),
        ('odessa', 'UKR/Circle/odessa', 'Odessa, Ukraine', int(1.2e5)),
        ('kharkiv', 'UKR/Circle/kharkiv', 'Kharkiv, Ukraine', int(1.419e6)),
        ('dnipro', 'UKR/Circle/dnipro', 'Dnipro, Ukraine', int(20271)),
        ('dublin', 'IRL/Circle/dublin', 'Dublin, Ireland', int(1024027)),
        ('sofia', 'BGR/Circle/sofia', 'Sofia, Bulgaria', int(1062065)),
        ('warsaw', 'POL/Circle/warsaw', 'Warsaw, PO', int(1651676)),
        ('katowice-urban-area', 'POL/Circle/katowice-urban-area', 'Katowice urban area, PO', int(2.7e6)),
        ('prague', 'CZE/Circle/prague', 'Prague, CZ', int(1154508)),
        ('budapest', 'HUN/Circle/budapest', 'Budapest, CZ', int(1708088)),
        ('bucharest', 'ROU/Circle/bucharest', 'Bucharest, Romania', int(1877155)),

        # GERMANY
        ('berlin', 'DEU/Circle/berlin', 'Berlin, DE', int(3398362)),
        ('hamburg', 'DEU/Circle/hamburg', 'Hamburg, DE', int(1733846)),
        ('munich', 'DEU/Circle/munich', 'Munich, DE', int(1246133)),
        ('cologne', 'DEU/Circle/cologne', 'Cologne, DE', int(968823)),
        ('ruhr-region-west', 'DEU/Circle/ruhr-region-west', 'Ruhr region west, DE', int(2.3e6)),
        ('ruhr-region-east', 'DEU/Circle/ruhr-region-east', 'Ruhr region east, DE', int(3.0e6)),

        # RUSSIA
        ('moscow', 'RUS/Circle/moscow', 'Moscow, RU', int(10381288)),
        ('saint-petersburg', 'RUS/Circle/saint-petersburg', 'Saint Petersburg, RU', int(4039751)),
        ('novosibirsk', 'RUS/Circle/novosibirsk', 'Novosibirsk, RU', int(1419016)),
        ('samara', 'RUS/Circle/samara', 'Samara, RU', int(1134742)),
        ('yekaterinburg', 'RUS/Circle/yekaterinburg', 'Yekaterinburg, RU', int(1287586)),
        ('rostov-on-don', 'RUS/Circle/rostov-on-don', 'Rostov-on-Don, RU', int(1.1e6)),
        ('chelyabinsk', 'RUS/Circle/chelyabinsk', 'Chelyabinsk, RU', int(1062931)),
        ('omsk', 'RUS/Circle/omsk', 'Omsk, RU', int(1129289)),
        ('kazan', 'RUS/Circle/kazan', 'Kazan, RU', int(1104750)),
        ('nizhny-novgorod', 'RUS/Circle/nizhny-novgorod', 'Nizhny Novgorod, RU', int(1.257e6)),

        # AMERICAS
        ('sao-paulo', 'BRA/Circle/sao-paulo', 'Sao Paulo, BR', int(10021437)),
        ('recife', 'BRA/Circle/recife', 'Recife, BR', int(1478118)),
        ('brasilia', 'BRA/Circle/brasilia', 'Brasilia, BR', int(2207812)),
        ('curitiba', 'BRA/Circle/curitiba', 'Curitiba, BR', int(1718433)),
        ('porto-alegre', 'BRA/Circle/porto-alegre', 'Porto Alegre, BR', int(1372763)),
        ('belo-horizonte', 'BRA/Circle/belo-horizonte', 'Belo Horizonte, BR', int(2373255)),
        ('fortaleza', 'BRA/Circle/fortaleza', 'Fortaleza, BR', int(2416920)),
        ('salvador', 'BRA/Circle/salvador', 'Salvador, BR', int(2711903)),
        ('mexico-city', 'MEX/Circle/mexico-city', 'Mexico City, MX', int(8.855e6)),
        ('rio-de-janeiro', 'BRA/Circle/rio-de-janeiro', 'Rio de Janeiro, BR', int(6023742)),
        ('buenos-aires', 'ARG/Circle/buenos-aires', 'Buenos Aires, AR', int(2.89e6)),
        ('santiago', 'CHL/Circle/santiago', 'Santiago, CL', int(4837248)),
        ('bogota', 'COL/Circle/bogota', 'Bogota, Columbia', int(7102602)),
        ('lima', 'PER/Circle/lima', 'Lima, Peru', int(7646786)),

        # AFRICA
        ('johannesburg', 'ZAF/Circle/johannesburg', 'Johannesburg, SA', int(2026466)),
        ('cape-town', 'ZAF/Circle/cape-town', 'Cape Town, SA', int(3433504)),
        ('pretoria', 'ZAF/Circle/pretoria', 'Pretoria, SA',int(7.41e5)),

)

#  sometimes we only want to do half the world
new_world_countries = ('USA', 'CAN', 'BRA', 'MEX', 'ARG', 'CHL', 'COL', 'PER')

proxies = {
    "http": "proxy.jpmchase.net:8443",
    "https": "proxy.jpmchase.net:8443",
}


class App(application.Application):
    base_url = 'https://api.midway.tomtom.com/ranking/live/'
    page_data_base = 'https://www.tomtom.com/en_gb/traffic-index/page-data/'

    def app_name(self):
        return 'scrape_traffic_data'

    def __init__(self, args):
        super().__init__()
        self.now = apptime.now()

        parser = argparse.ArgumentParser()
        parser.add_argument('--ustop20', type=application.str2bool, default=False,
                            help='If true (1), scrape top 20 us cities')
        parser.add_argument('--new_world_only', type=application.str2bool, default=False,
                            help='If true (1), scrape all cities in the new world')
        parser.add_argument('--old_world_only', type=application.str2bool, default=False,
                            help='If true (1), scrape all cities in the old world')
        parser.add_argument('--force_all', type=application.str2bool, default=False,
                            help='If true (1), scrape all locations')
        parser.add_argument('--min_hrs_since', type=int, default=8,
                            help='Only scrape locations we have not scraped in this many hours.')
        parser.add_argument('--max_errors', type=int, default=5,
                            help='Stop if we get this many errors.')
        parser.add_argument('--slug', nargs='+', required=False,
                            help='Just scrape these slugs (space separated)')
        parser.add_argument('-f', type=str)  # not used, but enables instantiating app from Jupyter Notebook
        args = parser.parse_args()

        n_options = args.force_all * 1 + (args.slug is not None) * 1 + args.ustop20 * 1
        n_options += args.new_world_only * 1 + args.old_world_only * 1

        if n_options > 1:
            raise ValueError('Only one of force_all, slug, ustop20, new_world_only, old_world_only can be specified.')

        self.args = args
        self.sql = oracle_db.Session()
        sql_base.SqlBase.metadata.create_all(oracle_db.engine)

    def run_application(self):
        n_errors = 0

        locs = LOCATIONS
        if self.args.slug:
            locs = [l for l in locs if l[0] in self.args.slug]

        if self.args.ustop20:
            locs = [l for l in locs if l[0] in slug_to_pop.keys()]

        if self.args.new_world_only:
            locs = [l for l in locs if l[1][:3] in new_world_countries]

        if self.args.old_world_only:
            locs = [l for l in locs if l[1][:3] not in new_world_countries]

        for slug, path, name, population in locs:
            loc = self.ensure_location_exists(slug, path, name, population)

            self.info(loc.slug)

            if self._check_need_to_scrape(loc) == False:
                self.info('Recently scraped, skipping.')
                continue

            try:
                self.scrape_location(loc)
            except Exception:
                self.logger.exception("Fatal error with {0}".format(slug))
                n_errors += 1
                if n_errors >= self.args.max_errors:
                    self.info('Hit max errors, exiting.')
                    return
            sleep_for = np.random.normal(10, 5)
            time.sleep(sleep_for if sleep_for > 0 else 10)
        self.info('Run complete.')

    def scrape_location(self, location):

        # they moved the 2019 data to a different json file.  let's get that first.
        page_data_path = self.page_data_base + '{0}-traffic/page-data.json'.format(location.slug)
        r = requests.get(page_data_path, proxies=proxies)
        if r.status_code != 200:
            self.info(r.text)
            assert r.status_code == 200  # raise an exception to be caught and counted in main loop
        try:
            data = r.json()
        except JSONDecodeError:
            self.info('Could not decode page-data.json file.  As this is expected for some cities, not counting this'
                      ' as an error.')
            return
        week_hours = data['result']['data']['citiesJson']['stats2019']['results']['weekHours']
        historical_lookup = {}
        for i, weekday in enumerate(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']):
            historical_lookup[i] = {}
            for hour in np.arange(24):
                historical_lookup[i][hour] = week_hours[weekday][hour]['congestion']

        time.sleep(.1)
        encoded_path = urllib.parse.urlencode({'foo': location.path}).replace('foo=', '')
        r = requests.get(self.base_url + encoded_path, proxies=proxies)
        if r.status_code != 200:
            self.info(r.text)
            assert r.status_code == 200  # raise an exception to be caught and counted in main loop

        data = r.json()
        n_records_added = 0

        for record in data['data']:
            ut = record['UpdateTime']
            chk = self.sql.query(TrafficStat).filter(
                TrafficStat.location_id == location.id).filter(
                TrafficStat.update_time == ut).first()
            if chk:
                continue
            ts = TrafficStat(
                location_id=location.id,
                jams_delay=record['JamsDelay'],
                traffic_index_live=record['TrafficIndexLive'],
                update_time=record['UpdateTime'],
                jams_length=record['JamsLength'],
                jams_count=record['JamsCount']
            )

            # lookup the historical value
            dt = datetime.utcfromtimestamp(record['UpdateTime'] / 1e3)
            dt = pytz.utc.localize(dt).astimezone(pytz.timezone(location.timezone))
            weekday, hour = dt.weekday(), dt.hour
            ts.traffic_index_historic = historical_lookup[weekday][hour]

            self.sql.add(ts)
            self.sql.commit()
            n_records_added += 1
        self.info('{0}: complete, {1} records added'.format(location.slug, n_records_added))

    def ensure_location_exists(self, slug, path, name, population):
        try:
            loc = self.sql.query(TrafficStatLocation).filter(TrafficStatLocation.slug == slug).one()
            if loc.path != path:  # update path if necessary
                loc.path = path
                self.sql.commit()
            if loc.name != name:  # update name if necessary
                loc.name = name
                self.sql.commit()
            if loc.population != population:
                loc.population = population
                self.sql.commit()
        except sqlalchemy.orm.exc.NoResultFound:
            loc = TrafficStatLocation(slug=slug, path=path, name=name, population=population)
            self.sql.add(loc)
            self.sql.commit()
        return loc

    def _check_need_to_scrape(self, location):

        if self.args.force_all:
            return True

        n_scraped = self.sql.query(TrafficStat).filter(TrafficStat.location_id == location.id).count()
        if n_scraped == 0:
            return True

        last_scraped = self.sql.query(func.max(TrafficStat.created_at)).filter(TrafficStat.location_id == location.id)
        last_scraped = last_scraped[0][0]
        time_since = datetime.utcnow() - last_scraped
        hrs_since = time_since.days * 24 + time_since.seconds / 3600
        return hrs_since > self.args.min_hrs_since

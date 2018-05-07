"""
Scrapes nba projections from rotogrinders
"""
import logging
import argparse
import sys
import bs4

from datetime import date
from collections import defaultdict
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QUrl
from PyQt5.QtWebKitWidgets import QWebPage
import pandas as pd


class Client(QWebPage):

    def __init__(self, url):
        self.app = QApplication(sys.argv)
        QWebPage.__init__(self)
        self.loadFinished.connect(self.on_page_load)
        self.mainFrame().load(QUrl(url))
        self.app.exec_()

    def on_page_load(self):
        self.app.quit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # use debug when changing rg tables cause breakage
    parser.add_argument('-v', '--verbose', dest='debug', action='store_true')
    args = parser.parse_args()

    logging.getLogger(__name__)
    if args.debug:
        logging.basicConfig(level='DEBUG')
    else:
        logging.basicConfig(level='INFO')
    logging.info('Starting')

    url = "https://rotogrinders.com/projected-stats?site=draftkings&sport=nba"

    client_response = Client(url)
    source = client_response.mainFrame().toHtml()
    soup = bs4.BeautifulSoup(source, 'lxml')
    table = soup.find('div', class_='rgtable')
    columns = table.find_all('div', class_='rgt-col')
    
    if args.debug:
        for i, col in enumerate(columns):
            logging.debug('Column %d', i)
            logging.debug(col)

    data = defaultdict(list)
   
    for col in columns:
        key = col.find('div').getText()
        logging.debug('key is %s', key)
        for val in col.find_all('div')[1:]:
            data[key].append(val.getText())

    logging.info('Keys: %s', data.keys())
    df = pd.DataFrame(data)
    logging.info('Dataframe shape is %s', df.shape)
    logging.info('Saving projections')
    df.to_csv(f'nba-{date.today()}.csv', index=False)
    logging.info('Done')
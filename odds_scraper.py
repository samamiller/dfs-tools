"""Async odds scraper.

This module will scrape historical odds for the: mlb, nba, nfl, and nhl.
"""
import argparse
import asyncio
import logging
from collections import defaultdict

import aiohttp
import bs4
import pandas as pd
import requests

NFL_HEADER = ['Date', 'Vs', 'Score', 'Week', 'Team Line', 'O/U']
NHL_HEADER = ['Date', 'Vs', 'Score', 'Goalie', 'Opp. Goalie', 'M/L', 'O/U']
NBA_HEADER = ['Date', 'Vs', 'Score', 'Type', 'Team Line', 'O/U']
MLB_HEADER = ['Date', 'Vs', 'Score', 'Away Starter', 'Home Starter', 'Team Line', 'O/U']

HEADERS = {'mlb': MLB_HEADER, 'nfl': NFL_HEADER, 'nhl': NHL_HEADER, 'nba': NBA_HEADER}
MIN_YEAR = {'mlb': 1999, 'nfl': 1985, 'nhl': 1995, 'nba': 1990}


def get_teams(sport):
    url = f'https://www.covers.com/pageLoader/pageLoader.aspx?page=/data/{sport}/teams/teams.html'
    response = requests.get(url)
    response.raise_for_status()
    soup = bs4.BeautifulSoup(response.text, 'lxml')
    return soup.select('td a')


async def download(semaphore, session, url_tuple, args):
    await semaphore.acquire()
    url, fname = url_tuple
    logging.debug(f'Acquired semaphore for {fname}')
    async with session.get(url) as response:
        if response.status == 200:
            soup = bs4.BeautifulSoup(await response.text(), 'lxml')
        else:
            raise ValueError(f'Response status not valid {url}')
    rows = soup.select('table tr')
    data = defaultdict(list)
    for row in rows:
        cols = row.select('td')
        for i, col in enumerate(cols):
            if col.get('class') == ['datahead'] or col.get('class') == ['datacellc']:
                break
            col = col.text.strip()
            if i == 2 and (args.sport == 'nba' or args.sport == 'nhl'):
                col = ' '.join(t.strip() for t in col.split('\r\n'))
            if args.sport == 'mlb' and (i == 3 or i == 4):
                col = ' '.join(t.strip() for t in col.split('\r\n'))
            if args.sport == 'nfl' and (i == 4 or i == 5):
                col = ' '.join(t.strip() for t in col.split('\n'))
            data[HEADERS[args.sport][i]].append(col)
    df = pd.DataFrame(data)
    df.to_csv(fname + '.csv', index=False)
    logging.debug(f'Releasing semaphore for {fname}')
    semaphore.release()


async def main(args):
    base_url = 'https://www.covers.com'
    urls = []
    teams = get_teams(args.sport)
    years = range(args.begin, args.end)
    for t in teams:
        link = t.get('href').split('/')
        team = t.text.replace(' ', '_').replace('.', '')
        for y in years:
            if args.sport == 'mlb':
                link = link[:-1] + ['pastresults', f'{y}'] + [link[-1]]
            else:
                link = link[:-1] + ['pastresults', f'{y}-{y+1}'] + [link[-1]]
            link = '/'.join(link)
            urls.append((base_url + link, f'{team}_{y}'))
    semaphore = asyncio.Semaphore(30)
    async with aiohttp.ClientSession() as session:
        await asyncio.wait([download(semaphore, session, url, args) for url in urls])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-b', '--begin', type=int, default=2016,
        help="Year to begin scraping from; MIN_YEAR = {'mlb': 1999, 'nfl': 1985, 'nhl': 1995, 'nba': 1990}"
    )
    parser.add_argument('-e', '--end', type=int, default=2017, help="Year to stop scraping at; Not included [begin, end)")
    parser.add_argument(
        '-s', '--sport', choices=['nfl', 'nhl', 'nba', 'mlb'], default='nfl', help="Chose which sport to scrape: 'nfl', 'nhl', 'nba', 'mlb'"
    )
    parser.add_argument('-v', '--verbose', action="store_true", help="Set logging level to DEBUG")
    args = parser.parse_args()

    logging.getLogger(__name__)
    if args.verbose:
        logging.basicConfig(level='DEBUG')
    else:
        logging.basicConfig(level='INFO')
    if args.begin < MIN_YEAR[args.sport]:
        args.begin = MIN_YEAR[args.sport]
        logging.info(f'Odds data for {args.sport} begins in {MIN_YEAR[args.sport]}.\n\tSetting --begin to {MIN_YEAR[args.sport]}.')
    if args.end <= args.begin:
        args.end = args.begin + 1
        logging.info(f'--end must be greater than --begin.\n\tSetting --end to {args.end}.')
    logging.info('Starting scraping')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
    loop.close()

    logging.info('Done')

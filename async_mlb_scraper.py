"""
Async mlb data scraper
"""
import argparse
import asyncio
import datetime
import logging
import os

import aiofiles
import aiohttp
import bs4


def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%m/%d/%Y").date()
    except Exception:
        raise argparse.ArgumentTypeError(f'{s} must be a date like 4/10/2006')


async def get_scoreboard(sem, session, url):
    await sem.acquire()
    logging.debug(f'Acquired semaphore:\n{url}')
    async with session.get(url) as response:
        if response.status == 200:
            soup = bs4.BeautifulSoup(await response.text(), 'lxml')
        else:
            raise ValueError(f'Response status not valid {url}')
    gids = soup.select('game')
    gids = ['gid_' + g.get('id') for g in gids]
    url = url[:-len('scoreboard.xml')]
    await asyncio.wait([download(session, url, gid, '/players.xml') for gid in gids])
    await asyncio.wait([download(session, url, gid, '/miniscoreboard.xml') for gid in gids])
    await asyncio.wait([download(session, url, gid, '/inning/inning_all.xml') for gid in gids])
    logging.debug(f'Releasing semaphore:\n{url}')
    sem.release()


async def download(session, url, gid, s):
    async with session.get(url + gid + s) as response:
        if response.status == 200:
            folder = s.split('/')[1].split('.')[0]
            filename = gid + '_' + s.split('/')[-1]
            async with aiofiles.open(os.path.join(folder, filename), 'wb') as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    await f.write(chunk)
            return await response.release()
        else:
            logging.info(f'Url response invalid:\n{url+gid+s}')


async def main(args):
    base_url = 'http://gd2.mlb.com/components/game/mlb'
    urls = []
    while args.start < args.end:
        d = f'/year_{args.start.year}/month_{args.start.month:02d}/day_{args.start.day:02d}'
        urls.append(base_url + d + '/scoreboard.xml')
        args.start += datetime.timedelta(days=1)
    semaphore = asyncio.Semaphore(15)
    async with aiohttp.ClientSession() as session:
        await asyncio.wait([get_scoreboard(semaphore, session, url) for url in urls])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', type=valid_date, default='4/3/2018', help='Scraping start date format: 1/1/2005')
    parser.add_argument('-e', '--end', type=valid_date, default=datetime.date.today(), help='Scraping end date format: 4/1/2018')
    parser.add_argument('-d', '--dir', default=os.getcwd(), help='Directory to save data in default: current working directory')
    parser.add_argument('-v', '--verbose', action="store_true", help='Turn on DEBUG')
    args = parser.parse_args()
    if args.end <= args.start:
        raise ValueError('--end must be greater than --start')

    logging.getLogger(__name__)
    if args.verbose:
        logging.basicConfig(level='DEBUG')
    else:
        logging.basicConfig(level='INFO')
    logging.info('Starting')

    os.chdir(args.dir)
    try:
        os.mkdir('players')
    except FileExistsError:
        pass
    try:
        os.mkdir('miniscoreboard')
    except FileExistsError:
        pass
    try:
        os.mkdir('inning')
    except FileExistsError:
        pass
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
    loop.close()

    logging.info('Done')

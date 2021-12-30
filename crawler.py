from typing import Optional
import logging
import os.path
import re
import sys
from optparse import OptionParser

import aiohttp
import asyncio
import aiofiles
import aiofiles.os
from bs4 import BeautifulSoup


MAIN_URL = 'http://news.ycombinator.com/'


def parse_main_page(html_page) -> tuple[list[str], list[str]]:
    article_urls = []
    comments_urls = []

    soup = BeautifulSoup(html_page, 'html.parser')

    titlelink_tags = soup.find_all('a', class_='titlelink')
    td_tags = soup.find_all('td', class_='subtext')
    for td_tag, titlelink_tag in zip(td_tags, titlelink_tags):
        comment_tag = td_tag.find_all('a', string=re.compile('comment'))
        article_urls.append(titlelink_tag.attrs.get('href'))
        comments_urls.append(comment_tag[0].attrs.get('href') if comment_tag else None)

    return article_urls, comments_urls


def parse_comments_page(html_page) -> list[str]:
    urls = []
    soup = BeautifulSoup(html_page, 'html.parser')

    comments = soup.find_all('span', {'class': 'commtext c00'})
    for c in comments:
        a_tags = c.find_all('a')
        if len(a_tags) > 0:
            for a_tag in a_tags:
                urls.append(a_tag.attrs.get('href'))

    return urls


async def get_page_by_url(session: aiohttp.ClientSession, url, options) -> str:
    for _ in range(options.max_retry):
        async with session.get(url) as response:
            if response.status != 200:
                await asyncio.sleep(options.time_retry)
                continue

            text = await response.text()
            logging.info(f'page from {url} was load')
            return text


async def save_page(res, filename):
    async with aiofiles.open(filename, 'wb') as f:
        while True:
            chunk = await res.content.read(1024)
            if not chunk:
                break
            await f.write(chunk)


async def download_html(session: aiohttp.ClientSession, url, folder, options):
    for _ in range(options.max_retry):
        async with session.get(url) as res:
            if res.status != 200:
                await asyncio.sleep(options.time_retry)
                continue

            filename = f'{folder}/{os.path.basename(url)}.html'
            if not await aiofiles.os.path.exists(folder):
                await aiofiles.os.mkdir(folder)

            await save_page(res, filename)
            logging.info(f'page from {url} was save in {folder} folder')
            return await res.release()


async def get_urls_from_main(session, options) -> tuple[list[str], list[str]]:
    main_page = await get_page_by_url(session, MAIN_URL, options)
    return parse_main_page(main_page)


async def get_urls_from_comments(session, comments_url, options) -> Optional[list[str]]:
    if comments_url is None:
        return
    comments_page = await get_page_by_url(session, comments_url, options)
    return parse_comments_page(comments_page)


def split_list(main_list: list, len_sublists):
    groups_of_lists: list[list[str]] = []
    for i_url, url in enumerate(main_list):
        if i_url % len_sublists == 0:
            groups_of_lists.append([])
        groups_of_lists[-1].append(url)
    return groups_of_lists


async def main(options):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=options.timeout)) as session:
        errors = []
        while True:
            if errors:
                logging.info('Error detected, quitting')
                return
            try:
                a_urls, c_urls = await get_urls_from_main(session, options)

                all_comment_urls = []
                for group in split_list(c_urls, options.max_connections):
                    tasks = []
                    for comments_url in group:
                        if not comments_url:
                            continue
                        tasks.append(get_urls_from_comments(
                            session=session,
                            comments_url=MAIN_URL + comments_url,
                            options=options
                        ))
                    all_comment_urls.extend(await asyncio.gather(*tasks))

                for group_a, group_c in zip(
                        split_list(a_urls, options.max_connections),
                        split_list(all_comment_urls, options.max_connections)
                ):
                    tasks = []
                    for article_url, comments_urls in zip(group_a, group_c):
                        folder = os.path.join(options.folder, os.path.basename(article_url))
                        if await aiofiles.os.path.exists(folder):
                            continue

                        tasks.append(download_html(session, article_url, folder, options))
                        for c_url in comments_urls:
                            tasks.append(download_html(session, c_url, folder, options))
                    await asyncio.gather(*tasks)

            except Exception as e:
                logging.error(f'Unexpected error: {e}')
                errors.append(e)

            await asyncio.sleep(options.time_update)

if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--folder", action="store", default="output")
    op.add_option("--timeout", action="store", default=3)
    op.add_option("--max_retry", action="store", default=3)
    op.add_option("--time_retry", action="store", default=1)
    op.add_option("--time_update", action="store", default=10)
    op.add_option("--max_connections", action="store", default=10)
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    logging.info("Ycrawler started with options: %s" % opts)
    try:
        asyncio.run(asyncio.wait_for(main(opts), opts.time_update))
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)

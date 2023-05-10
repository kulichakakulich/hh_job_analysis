import aiohttp
import asyncio
import fake_useragent
import json
from aiofiles import open as aio_open
from glob import glob
import os

from add.settings import text_filter, pagination_folder, vacancies_folder


async def get_page(text, pg=0):
    params = {
        'text': text,
        'area': [2, 4],
        'page': pg,
        'per_page': 100
    }
    url = 'https://api.hh.ru/vacancies'
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, headers=get_headers()) as response:
            data = await response.json()
    return data


async def save_json(data, filename):
    async with aio_open(filename, mode='w', encoding='utf8') as f:
        await f.write(data)


def get_headers():
    user_ag = fake_useragent.UserAgent().random
    headers = {'user-agent': user_ag}
    return headers


async def scrape_vacancies():
    files = glob(f"{pagination_folder}/*.json")
    latest_file = max(files, key=os.path.getctime)
    async with aio_open(latest_file, encoding='utf8') as f:
        json_dict = json.loads(await f.read())
    tasks = []
    for vac in json_dict['items']:
        tasks.append(scrape_vacancy(vac))
    await asyncio.gather(*tasks)
    print(f'Processed {len(tasks)} vacancies')
    print('Vacancies scraped')


async def scrape_vacancy(vac):
    async with aiohttp.ClientSession() as session:
        async with session.get(vac['url'], headers=get_headers()) as response:
            data = await response.text()
    filename = f"{vacancies_folder}/{vac['id']}.json"
    await save_json(data, filename)
    await asyncio.sleep(0.25)


async def scrape_pages():
    for page in range(10):
        page_dict = await get_page(text_filter, page)

        filename = f"{pagination_folder}/{len(glob(f'{pagination_folder}/*.json'))}.json"
        await save_json(json.dumps(page_dict, ensure_ascii=False), filename)

        if (page_dict['pages'] - page) <= 1:
            break
        await asyncio.sleep(0.25)
    print('Pages scraped')


async def main():
    await scrape_pages()
    await scrape_vacancies()


if __name__ == '__main__':
    asyncio.run(main())

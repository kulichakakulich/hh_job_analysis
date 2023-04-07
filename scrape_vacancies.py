import aiohttp
import asyncio
import fake_useragent
import json
from aiofiles import open as aio_open
from glob import glob


text_filter = '"Data Engineer" OR "ML Engineer" OR "ETL Developer"'
pagination_folder = './docs/pagination'
vacancies_folder = './docs/vacancies'
sleep_duration = 0.25


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
    i = 0
    for file in glob(f"{pagination_folder}/*.json"):
        async with aio_open(file, encoding='utf8') as f:
            json_dict = json.loads(await f.read())
        vac_count = json_dict['found']
        tasks = []
        for vac in json_dict['items']:
            tasks.append(scrape_vacancy(vac))
        await asyncio.gather(*tasks)
        i += len(tasks)
        print(f'Processed {i} of {vac_count} vacancies')
    print('Vacancies scraped')


async def scrape_vacancy(vac):
    async with aiohttp.ClientSession() as session:
        async with session.get(vac['url'], headers=get_headers()) as response:
            data = await response.text()
    filename = f"{vacancies_folder}/{vac['id']}.json"
    await save_json(data, filename)
    await asyncio.sleep(sleep_duration)


async def scrape_pages():
    for page in range(10):
        page_dict = await get_page(text_filter, page)

        filename = f"{pagination_folder}/{len(glob(f'{pagination_folder}/*.json'))}.json"
        await save_json(json.dumps(page_dict, ensure_ascii=False), filename)

        if (page_dict['pages'] - page) <= 1:
            break
        await asyncio.sleep(sleep_duration)
    print('Pages scraped')


async def main():
    await scrape_pages()
    await scrape_vacancies()


if __name__ == '__main__':
    asyncio.run(main())

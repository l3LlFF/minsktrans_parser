import time
import asyncio
import aiohttp
import re
import json
from bs4 import BeautifulSoup
import pandas as pd
from decouple import config
import aiofiles
import os
import random



NUM_PAGES = int(config('NUM_PAGES', 2))
NUM_SEMAPHORES = int(config('NUM_SEMAPHORES', 5))
SVDIR = config('IMAGES_FOLDER', '/')


durations = []
hrefs = []
result = []
image_refs = []
vehicle_numbers = []

cookies = {
    'ASP.NET_SessionId': '5o3in0zxss1sssimdpaagzqj',
    '__RequestVerificationToken_L2xvb2tvdXRfeWFyZA2': 'eOGisuCY6u8oOITivSi5sx_IOWqTy_9SASPgTa4-z03WNB-RhW-D0TEC4sYTFNs3RjnEfkm5K3iKKSqhkfsGc38kfpAYdcGr4hFyBG8pH1g1',
    'pll_language': 'ru',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:94.0) Gecko/20100101 Firefox/94.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://minsktrans.by',
    'Connection': 'keep-alive',
    'Referer': 'https://minsktrans.by/lookout_yard/Home/Index/minsk',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}

def scratch(content):
    html = content.replace('\n', '').replace('\t', '')
    soup = BeautifulSoup(html, 'lxml')
    district = street = object_type = x_area = region = city = ''
    for el in soup.find_all('table'):
        tr = el.find_all('tr')

        for e in tr:
            td = e.find_all('td')
            if len(td) < 2:
                continue
            key = td[0].text
            value = td[1].text
            if key == 'Район города':
                try:
                    district = td[1].find('a').text
                except:
                    district = ''
            elif key == 'Адрес':
                street = value
            elif key == 'Вид объекта':
                object_type = value
            elif key == 'Площадь':
                x_area = value
            elif key == 'Область':
                region = value
            elif key == 'Населенный пункт':
                city = value
    try:
        phone = soup.find('div', {'class': 'object-contacts'}).find('strong').text
    except:
        phone = ''
    price_block = soup.find('a', {'data-currency': '840', 'rel': 'tooltip'})
    if price_block is None:
        price = ''
        price_per_meter = ''
    else:
        price = price_block['data-price'].replace(' ', '')
        price_per_meter = price_block['data-price_m2'].replace(' ', '')
        if 'м²' in price:
            price_per_meter = price
            price = ''
        if price != '':
            price = re.match(r'[a-zA-ZА-Яа-я]*([0-9.,]+)', price).group(1)
        if price_per_meter != '':
            price_per_meter = re.match(r'[a-zA-ZА-Яа-я]*([0-9.,]+)', price_per_meter).group(1)

    location = soup.find('div', {'id': 'map-center'})
    if location is None:
        lon = ''
        lat = ''
        point = ''
    else:
        position_block = json.loads(location['data-center'])['position.']
        lon = position_block['x']
        lat = position_block['y']
        point = f"'SRID=4326;POINT ({lon} {lat})'::geometry"
    description = str(soup.find('div', {'class': 'top-description'}))
    try:
        agency = soup.find('div', {'class': 'agency-info-left'}).find('strong').text
    except:
        agency = ""
    images = [x['data-src'] for x in soup.find_all('a', {'class': 'object-gallery-item'})]
    return [point, district, street, object_type, x_area, region, city,
                 description, phone, price, price_per_meter, agency], images


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def timed(func):
    """
    records approximate durations of function calls
    """
    def wrapper(*args, **kwargs):
        start = time.time()
        print(f'{func.__name__:<30} started')
        result = func(*args, **kwargs)
        duration = f'{func.__name__:<30} finished in {time.time() - start:.2f} seconds'
        print(duration)
        durations.append(duration)
        return result
    return wrapper


def get_proxy(filename):
    with open(filename, 'r') as f:
        data = f.read().splitlines()
        return [x.split(':') for x in data]


async def fetch_hrefs(vehicle_type, session, proxy):
    """
    asynchronous get request
    """
    host, port, login, password = proxy
    proxy_auth = aiohttp.BasicAuth(login, password)
    data = {
        'p': 'minsk',
        'tt': vehicle_type,
        '__RequestVerificationToken': 'VqPstx8rJoSp_ReF0nP7Kj9aG0mk3rsW4Nl0F8B2Xm_zHs3oBef4vp2KdGbCKWmN2wtNEbqprQQilVL0lH7jbsQ8e2uLonFIBRcjgaipVkA1'
    }
    print(vehicle_type)
    async with session.post('https://minsktrans.by/lookout_yard/Data/RouteList',
                           proxy=f"http://{host}:{port}",
                           proxy_auth=proxy_auth,
                           headers=headers,
                           cookies=cookies,
                           data=data) as response:
        response_json = await response.json()
        return vehicle_numbers.extend([(vehicle_type, x['Number']) for x in response_json['Routes']])


async def fetch_data(data, session, proxy):
    """
    asynchronous get request
    """
    host, port, login, password = proxy
    proxy_auth = aiohttp.BasicAuth(login, password)

    data = {
        'p': 'minsk',
        'tt': data[0],
        'r': data[1],
        '__RequestVerificationToken': 'K9JeD-LIjFqk7Y69WHTxJd5-vmX9hIxSXKZnkmq77dyIXCukJ8x0k3h_hFSE3vu4P0QHCN-brtLvy3ej4opqwX4LxCs2zD4Oib1suRRYhW81',
        'v': '21099'
    }

    async with session.post('https://minsktrans.by/lookout_yard/Data/Vehicles',
                            proxy=f"http://{host}:{port}",
                            proxy_auth=proxy_auth,
                            headers=headers,
                            cookies=cookies,
                            data=data) as response:
        try:
            response_json = await response.json()
            info = [(x['Id'], x['IdEndStop'], x['TripType'], f"SRID=4326;POINT({x['Longitude']} {x['Latitude']})", x['IsApparel']) for x in
                    response_json['Vehicles']]
            result.extend(info)
        except aiohttp.client_exceptions.ContentTypeError as e:
            pass



async def fetch_image(url, session, proxy):
    """
    asynchronous get request
    """
    host, port, login, password = proxy
    proxy_auth = aiohttp.BasicAuth(login, password)
    id, href = url
    if not os.path.exists(SVDIR):
        os.makedirs(SVDIR)
    folder = os.path.join(SVDIR, id)
    if not os.path.exists(folder):
        os.makedirs(folder)
        async with session.get(href, proxy=f"http://{host}:{port}",
                               proxy_auth=proxy_auth) as response:
            if response.status == 200:
                filename = f"{random.getrandbits(32)}.jpg"
                sv_path = os.path.join(SVDIR, id, filename)
                f = await aiofiles.open(sv_path, mode='wb')
                await f.write(await response.read())
                await f.close()


async def gather_tasks(loop, urls, function, proxies, n_semaphores):
    """
    gathers tasks
    """
    async def sem_task(task, semaphore):
        async with semaphore:
            await task

    for proxy, url_chunk in zip(proxies, urls):
        semaphore = asyncio.Semaphore(n_semaphores)
        async with aiohttp.ClientSession(trust_env=True, connector=aiohttp.TCPConnector(limit=64, ssl=False)) as session:
            tasks = [loop.create_task(sem_task(function(url, session, proxy), semaphore)) for url in url_chunk]
            return await asyncio.gather(*(task for task in tasks))


@timed
def async_run(urls, function, proxies, n_semaphores):
    """
    performs asynchronous function
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        gather_tasks(loop, urls, function, proxies, n_semaphores)
    )

def clear_list(lst):
    """
    clears list from None values
    """
    length = len(lst)
    cleared_list = [x for x in lst if len(x) != 0]
    print(f'Links found: {len(cleared_list)}')
    print(f'Links lost: {length - len(cleared_list)}')
    return cleared_list


def to_database(data):

    urls = f"""postgresql://{config('DB_LOGIN')}:{config('DB_PASSWORD')}@{config('DB_HOST')}:{config('DB_PORT')}/{config('DB_NAME')}"""
    from sqlalchemy import create_engine
    engine = create_engine(urls)

    sqls = [
        """
        CREATE TABLE public.minsktrans (
        id int8 NULL,
        id_end_stop int8 NULL,
        trip_type int8 NULL,
        way geometry NULL,
        date_added timestamp default now()
        )
        """,
        f"""
        CREATE INDEX minsktrans_y ON public.minsktrans USING gist (way);    
        """
        ]

    for sql in sqls:
        with engine.begin() as conn:
            try:
                conn.execute(sql)
            except Exception as e:
                print(e)
                continue



    values = ','.join([f"""({i['id']}, {i['IdEndStop']}, {i['TripType']}, '{i['way']}')"""
                       for i in list(data.to_records(index=False))])
    s = f"""
    INSERT INTO minsktrans (id, id_end_stop, trip_type, way)
    values {values} 
    """
    with engine.begin() as conn:
        conn.execute(s)




if __name__ == '__main__':

    vehicles = ['tram']
    print(f"Started {type} objects parsing")
    proxies = get_proxy('proxy.txt')
    chunked_urls = chunks(vehicles, len(vehicles) // len(proxies))
    print(f"fetching links")
    async_run(urls=chunked_urls, function=fetch_hrefs,
              proxies=proxies, n_semaphores=NUM_SEMAPHORES)
    print(f"fetched {len(vehicle_numbers)} vehicles")
    chunked_urls = chunks(vehicle_numbers, len(vehicle_numbers) // len(proxies))
    print(f"fetching links")
    async_run(urls=chunked_urls, function=fetch_data,
              proxies=proxies, n_semaphores=NUM_SEMAPHORES)
    clear_list(result)
    df = pd.DataFrame(result, columns=['id', 'IdEndStop', 'TripType', 'way', 'IsApparel'])
    print(df)
    to_database(df)
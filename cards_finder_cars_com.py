import random

import pymysql

from bs4 import BeautifulSoup
import requests
import json
import time
import os


start_time = time.time()
start_time_str = time.strftime('%Y-%m-%d-%H-%M-%S', time.gmtime(start_time))

headers = requests.utils.default_headers()
headers.update({
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'en-US,en;q=0.8',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'TGTG/22.2.1 Dalvik/2.1.0 (Linux; U; Android 9; SM-G955F Build/PPR1.180610.011)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive'
})

DEFAULT_HEADER = headers #{'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
SITE_URL = "https://www.cars.com"


def get_card_url_list(url, site_url=SITE_URL, headers=DEFAULT_HEADER):
    url_list = []

    page = requests.get(url, headers=headers)
    if page.status_code == 200:
        soup = BeautifulSoup(page.text, "html.parser")

        listing_items = soup.find_all("div", class_="vehicle-card")
        try:
            for item in listing_items:
                item_href = item.find("a", class_="image-gallery-link")["href"]
                url_list.append(site_url + item_href)
        except:
            pass

    return url_list

def make_folder(start_folder, subfolders_chain):
    folder = start_folder
    for subfolder in subfolders_chain:
        folder += "/" + subfolder
        if not os.path.isdir(folder):
            os.mkdir(folder)

    return folder


def progress(total, left):
    return ((total-left)/total) * 100

def main():
    with open("config.json") as config_file:
        configs = json.load(config_file)

    con = pymysql.connect(**configs["audit_db"])

    # make_folder(configs["folders"]["logs"], ["cars_com", start_time_str])
    # make_folder(configs["folders"]["scrapped_data"], ["cars_com", "json", start_time_str])

    # LOG_FILENAME_CARS_COM = f"{configs['folders']['logs']}/cars_com/{start_time_str}/cards_finder_cars_com_log.txt"

    # with open(LOG_FILENAME_CARS_COM, 'w', newline="", encoding="utf-8") as log_file, con:
    with con:
        # print(f"start time (GMT): {time.strftime('%X', time.gmtime())}", file=log_file)

        cur = con.cursor()

        cur.execute(
            """
                insert into process_log(process_desc, user, host) 
                select 'cards_finder_cars_com.py' as process_desc, user, host 
                from information_schema.processlist 
                where ID=connection_id();
            """
        )

        cur.execute("select LAST_INSERT_ID() as process_log_id")
        process_log_id = cur.fetchone()[0]

        url_num = 0
        curr_year = int(time.strftime("%Y", time.gmtime()))

        combinations_to_process_dict = {}
        for year in range(curr_year, 1899, -1):
            for price_usd in range(0, 500001, 10000):
                for page_num in range(1, 101):
                    combinations_to_process_dict[(year, price_usd, page_num)] = 0

        total_combinations = combinations_to_process_left = len(combinations_to_process_dict)
        print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, ready to start")
        print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, combinations to process: {combinations_to_process_left}, progress: {round(progress(total_combinations, combinations_to_process_left), 3)}%")

        while combinations_to_process_left > 0:
            year = random.randint(1900, curr_year)
            price_usd = random.randint(0, 50)*10000
            page_num = random.randint(1, 100)

            if combinations_to_process_dict[(year, price_usd, page_num)] != 0:
                continue

            url = f"{SITE_URL}/shopping/results/?list_price_max={price_usd + 9999}&list_price_min={price_usd}&maximum_distance=all&page_size=100&page={page_num}&stock_type=used&year_max={year}&year_min={year}&zip=60606"

            # print(f"\ntime: {time.strftime('%X', time.gmtime(time.time() - start_time))}, url: {url}", file=log_file)
            print(f"\ntime: {time.strftime('%X', time.gmtime(time.time() - start_time))}, url: {url}")

            card_url_list = get_card_url_list(url)
            if card_url_list == []:
                # print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, no cards found", file=log_file)
                combinations_to_process_left -= 101 - page_num
                for page in range(page_num, 101):
                    combinations_to_process_dict[(year, price_usd, page)] = 1

                print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, no cards found. mark url search parameters ({year}, {price_usd}, {price_usd + 9999}, {page_num}+) to skip processing")
                print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, combinations left: {combinations_to_process_left}, progress: {round(progress(total_combinations, combinations_to_process_left), 3)}%")

                continue

            sql_statements = [
                f"""                    
                    insert into ad_groups(year, page_size, page_num, price_min, process_log_id) 
                    values({year}, 100, {page_num}, {price_usd}, {process_log_id});
                """,
                f"""
                    insert into car_ads_db.ads(source_id, card_url, ad_group_id, insert_process_log_id) 
                    with cte_new_urls(card_url)
                    as (
                         values {",".join([f"row('{url[len(SITE_URL):]}')" for url in card_url_list])}
                    ),
                    cte_url_last_status 
                    as (
                         select source_id, card_url, min(ad_status) as ad_status
                         from car_ads_db.ads
                         join cte_new_urls cte_new on ads.card_url = cte_new.card_url and
                                                      ads.source_id = '{SITE_URL}'
                         group by source_id, card_url
                    )
                    select '{SITE_URL}' as source_id, cte_new.card_url, LAST_INSERT_ID() as ad_group_id, {process_log_id}
                    from cte_new_urls cte_new
                    left join cte_url_last_status cte_existing on 
                                     cte_new.card_url = cte_existing.card_url and
                                     cte_existing.source_id = '{SITE_URL}'
                    where (cte_existing.ad_status is null) or (cte_existing.ad_status = 2);
                """]
            for sql in sql_statements:
                cur.execute(sql)

            # print(*card_url_list, sep="\n", file=log_file)
            combinations_to_process_dict[(year, price_usd, page_num)] = 2

            if len(card_url_list) < 100:
                for page in range(page_num+1, 101):
                    combinations_to_process_dict[(year, price_usd, page)] = 1

                combinations_to_process_left -= 100 - page_num
                print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, # of cards found ({len(card_url_list)}) < page_size (100). mark url search parameters ({year}, {price_usd}, {price_usd + 9999}, {page_num + 1}+) to skip processing")
                print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, combinations left: {combinations_to_process_left}, progress: {round(progress(total_combinations, combinations_to_process_left), 3)}%")
            else:
                combinations_to_process_left -= 1
                print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, combinations left: {combinations_to_process_left}, progress: {round(progress(total_combinations, combinations_to_process_left), 3)}%")

        # print(f"\nend time (GMT): {time.strftime('%X', time.gmtime())}", file=log_file)
        print(f"\nend time (GMT): {time.strftime('%X', time.gmtime())}")

        cur.execute(f"update process_log set end_date = current_timestamp where process_log_id = {process_log_id};")


if __name__ == "__main__":
    main()

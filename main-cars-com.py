from bs4 import BeautifulSoup
import requests
import csv
import time
import json
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

CSV_CARD_FILENAME_CARS_COM = f"scrapped_cards/CARS_COM/CSV/{start_time_str}/CARS_COM_card.csv"
CSV_CARD_GALLERY_FILENAME_CARS_COM = f"scrapped_cards/CARS_COM/CSV/{start_time_str}/CARS_COM_card_gallery.csv"
CSV_CARD_OPTIONS_FILENAME_CARS_COM = f"scrapped_cards/CARS_COM/CSV/{start_time_str}/CARS_COM_card_options.csv"
CSV_CARD_URL_CARS_COM = f"scrapped_cards/CARS_COM/CSV/{start_time_str}/CARS_COM_card_url.csv"
LOG_FILENAME_CARS_COM = f"./logs/CARS_COM/{start_time_str}/CARS_COM_log.txt"


def get_parsed_card(url, debug=0, headers=DEFAULT_HEADER):
    card_dict = {}

    page = requests.get(url, headers=headers)

    if page.status_code == 200:
        soup = BeautifulSoup(page.text, "html.parser")

        card = soup.find("section", class_="listing-overview")
        # print(card,"\n")

        card_gallery = card.find("div", class_="modal-slides-and-controls")
        card_dict["gallery"] = []
        try:
            for img in card_gallery.find_all("img", class_="swipe-main-image"):
                card_dict["gallery"].append(img["src"])
        except:
            pass


        basic_content = soup.find("div", class_="basics-content-wrapper")

        basic_section = basic_content.find("section", class_="sds-page-section basics-section")
        fancy_description_list = basic_section.find("dl", class_="fancy-description-list")
        dt_elements = [elem.text.strip() for elem in fancy_description_list.find_all("dt")]
        dd_elements = [elem.get_text(separator='|', strip=True).split("|")[0] for elem in fancy_description_list.find_all("dd")]
        for key, value in zip(dt_elements, dd_elements):
            card_dict[key.lower()] = value

        card_dict["card_id"] = card_dict.get("stock #")
        if not card_dict["card_id"]:
            card_dict["card_id"] = card_dict.get("vin")

        card_dict["url"] = url

        card_title = card.find(class_="listing-title")
        card_dict["title"] = card_title.text

        card_price_primary = card.find("div", class_="price-section")
        card_dict["price_primary"] = card_price_primary.find("span", class_="primary-price").text

        price_history = ""
        card_price_history = soup.find("div", class_="price-history")
        try:
            card_price_history_rows = card_price_history.find_all("tr")
            for row in card_price_history_rows:
                date, _, price = row.find_all("td")
                price_history += f"{date.text}: {price.text} | "

            card_dict["price_history"] = price_history[0:-2]
        except:
            card_dict["price_history"] = ""

        card_dict["options"] = []
        try:
            feature_content = basic_content.find("section", class_="sds-page-section features-section")
            fancy_description_list = feature_content.find("dl", class_="fancy-description-list")
            dt_elements = [elem.text.strip() for elem in fancy_description_list.find_all("dt")]
            dd_elements = [elem.get_text(separator='|', strip=True).split("|") for elem in fancy_description_list.find_all("dd")]
            for category, values in zip(dt_elements, dd_elements):
                section_dict = {}
                section_dict["category"] = category
                section_dict["items"] = values

                card_dict["options"].append(section_dict)

            all_features = basic_content.find("div", class_="all-features-text-container")
            section_dict = {}
            section_dict["category"] = "features"
            section_dict["items"] = all_features.get_text("|", True).split("|")
            card_dict["options"].append(section_dict)
        except:
            pass


        try:
            card_vehicle_history = basic_content.find("section", class_="sds-page-section vehicle-history-section")
            fancy_description_list = card_vehicle_history.find("dl", class_="fancy-description-list")
            dt_elements = [elem.text.strip() for elem in fancy_description_list.find_all("dt")]
            dd_elements = [elem.get_text(separator='|', strip=True) for elem in fancy_description_list.find_all("dd")]
            vehicle_history = ""
            for record, value in zip(dt_elements, dd_elements):
                vehicle_history += f"{record}: {value} | "

            card_dict["vehicle_history"] = vehicle_history[0:-2]
        except:
            card_dict["vehicle_history"] = ""

        card_comment = basic_content.find("div", class_="sellers-notes")
        try:
            card_dict["comment"] = card_comment.get_text(separator="|", strip=True)
        except:
            card_dict["comment"] = ""

        card_location = basic_content.find("div", class_="dealer-address")
        try:
            card_dict["location"] = card_location.text
        except:
            card_dict["location"] = ""

        card_labels_div = card.find("div", class_="vehicle-badging")
        data_override_payload_json = json.loads(card_labels_div["data-override-payload"])
        card_dict["bodystyle"] = data_override_payload_json["bodystyle"]

        labels = []
        try:
            for div in card_labels_div.find_all("span", class_="sds-badge__label"):
                labels += [div.text]

            labels += ["VIN: " + card_dict["vin"]]

            if basic_content.find("section", "sds-page-section warranty_section"):
                labels += ["Included warranty"]
        except:
            pass
        card_dict["labels"] = "|".join(labels)

        mpg = ""
        try:
            mpg = card_dict.get("mpg").strip().replace('0–0', "")
            if mpg == "–":
                mpg = ""
        except:
            pass

        card_dict["description"] = card_dict["title"].split()[0] + ", " + \
                                   card_dict["transmission"] + ", " + \
                                   card_dict["engine"] + ", " + \
                                   card_dict["fuel type"] + \
                                   ((" (" + mpg + " mpg)") if mpg else "") + ", " + \
                                   card_dict["mileage"].replace(",", ".") +" | " + \
                                   card_dict["bodystyle"] + ", " + \
                                   card_dict["drivetrain"] + ", " + \
                                   card_dict["exterior color"]

        del card_dict["transmission"]
        del card_dict["engine"]
        del card_dict["fuel type"]
        del card_dict["mileage"]
        del card_dict["bodystyle"]
        del card_dict["drivetrain"]
        del card_dict["exterior color"]
        del card_dict["interior color"]
        if card_dict.get("mpg"):
            del card_dict["mpg"]
        del card_dict["vin"]
        if card_dict.get("stock #"):
            del card_dict["stock #"]

        # card_dict["exchange"] = ""

        card_dict["scrap_date"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

        card_dict["json"] = card_dict.copy()

        del card_dict["url"]

    return card_dict

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

def main():
    make_folder(f"{os.curdir}", ["scrapped_cards", "CARS_COM", "JSON", start_time_str])
    make_folder(f"{os.curdir}", ["scrapped_cards", "CARS_COM", "CSV", start_time_str])
    make_folder(f"{os.curdir}", ["logs", "CARS_COM", start_time_str])

    with \
            open(LOG_FILENAME_CARS_COM, 'w', newline="", encoding="utf-8") as log_file, \
            open(CSV_CARD_FILENAME_CARS_COM, 'a', newline="", encoding="utf-8") as card_csvfile, \
            open(CSV_CARD_GALLERY_FILENAME_CARS_COM, 'a', newline="", encoding="utf-8") as card_gallery_csvfile, \
            open(CSV_CARD_OPTIONS_FILENAME_CARS_COM, 'a', newline="", encoding="utf-8") as card_options_csvfile, \
            open(CSV_CARD_URL_CARS_COM, 'a', newline="", encoding="utf-8") as card_url_csvfile:

        print(f"start time (GMT): {time.strftime('%X', time.gmtime())}", file=log_file)

        card_fieldnames = "card_id,title,price_primary,price_history,location,labels,comment,description,vehicle_history,scrap_date".split(",")
        card_writer = csv.DictWriter(card_csvfile, fieldnames=card_fieldnames)
        card_writer.writeheader()

        card_gallery_fieldnames = "card_id,ind,url,scrap_date".split(",")
        card_gallery_writer = csv.DictWriter(card_gallery_csvfile, fieldnames=card_gallery_fieldnames)
        card_gallery_writer.writeheader()

        card_options_fieldnames = "card_id,category,item,scrap_date".split(",")
        card_options_writer = csv.DictWriter(card_options_csvfile, fieldnames=card_options_fieldnames)
        card_options_writer.writeheader()

        card_url_fieldnames = "card_id,url,scrap_date".split(",")
        card_url_writer = csv.DictWriter(card_url_csvfile, fieldnames=card_url_fieldnames)
        card_url_writer.writeheader()

        url_num = 0
        curr_year = int(time.strftime("%Y", time.gmtime()))

        for year in range(curr_year, 1900, -1):
            for price_usd in range(0, 500001, 10000):
                for page_num in range(1, 500):
                    url = f"{SITE_URL}/shopping/results/?list_price_max={price_usd + 9999}&list_price_min={price_usd}&maximum_distance=all&page_size=20&page={page_num}&stock_type=used&year_max={year}&year_min={year}&zip=60606"

                    print(f"\ntime: {time.strftime('%X', time.gmtime(time.time() - start_time))}, url: {url}", file=log_file)
                    print(f"\ntime: {time.strftime('%X', time.gmtime(time.time() - start_time))}, url: {url}")

                    card_url_list = get_card_url_list(url)
                    if card_url_list == []:
                        print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, no cards found", file=log_file)
                        print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, no cards found")
                        break
                    else:
                        print(*card_url_list, sep="\n", file=log_file)

                    for url in card_url_list:
                        url_num += 1

                        print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, card: {url_num}, year: {year}: {url}", file=log_file)
                        print(f"time: {time.strftime('%X', time.gmtime(time.time() - start_time))}, card: {url_num}, year: {year}: {url}")

                        url_parts = url.split("?")
                        url_updated = url_parts[0].replace("/", "-").replace(".", "-").replace(":", "-")

                        parsed_card = {}
                        # try:
                        if len(url_parts) == 1:
                            parsed_card = get_parsed_card(url)
                        # except:
                        #     parsed_card = {}

                        if parsed_card == {}:
                            continue

                        card_id = parsed_card["card_id"]

                        folder = make_folder(f"{os.curdir}",
                                             ["scrapped_cards", "CARS_COM", "JSON",
                                              f"{start_time_str}",
                                              f"{year}",
                                              f"price_{price_usd}-{price_usd + 9999}"])
                        with open(f"{folder}/{url_updated}.json", "w", encoding="utf-8") as f:
                            f.write(str(parsed_card["json"]).replace("\\xa0", " ").replace("\\u2009", " "))

                        parsed_card_csv = parsed_card.copy()

                        del parsed_card_csv["gallery"]
                        del parsed_card_csv["options"]
                        del parsed_card_csv["json"]
                        for key, value in parsed_card_csv.items():
                            parsed_card_csv[key] = value.replace("\u00a0", " ").replace("\u2009", " ")
                        card_writer.writerow(parsed_card_csv)

                        card_url_csv_row = {"card_id": card_id, "url": url, "scrap_date": parsed_card["scrap_date"]}
                        card_url_writer.writerow(card_url_csv_row)

                        for img_ind, img_url in enumerate(parsed_card["gallery"]):
                            card_gallery_csv_row = {"card_id": card_id, "ind": img_ind + 1, "url": img_url,
                                                    "scrap_date": parsed_card["scrap_date"]}
                            card_gallery_writer.writerow(card_gallery_csv_row)

                        for section in parsed_card["options"]:
                            category = section["category"]
                            for item in section["items"]:
                                card_options_csv_row = {"card_id": card_id, "category": category, "item": item,
                                                        "scrap_date": parsed_card["scrap_date"]}
                                card_options_writer.writerow(card_options_csv_row)

                    if len(card_url_list) < 20:
                        break

        print(f"\nend time (GMT): {time.strftime('%X', time.gmtime())}", file=log_file)


if __name__ == "__main__":
    main()

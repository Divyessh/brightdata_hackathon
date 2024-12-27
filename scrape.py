import pandas as pd
from selenium.webdriver import Remote, ChromeOptions, Chrome, Safari
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection
from selenium.webdriver.common.by import By

import argparse
from multiprocessing import Pool
import os


SBR_WEBDRIVER = os.getenv("SELENIUM_KEY")
if not SBR_WEBDRIVER:
    print("Please configure your Bright Data key")
    exit()


def scrape(page_num: int):
    sbr_connection = ChromiumRemoteConnection(SBR_WEBDRIVER, "goog", "chrome")
    with Remote(sbr_connection, options=ChromeOptions()) as driver:
        driver.get(f"https://www.coingecko.com/?page={page_num}&items=300")
        table = driver.find_element(
            By.XPATH, '//table[@data-coin-table-target="table"]'
        )
        table_html = table.get_attribute("outerHTML")
        dfs = pd.read_html(str(table_html))
        dfs[0].to_csv(f"data/coin_gecko_{page_num}.csv")


def analyse(num_of_pages):
    merged_dfs_list = []
    for page in range(1, num_of_pages + 1):
        try:
            load_df = pd.read_csv(f"data/coin_gecko_{page}.csv")
            merged_dfs_list.append(load_df)
        except FileNotFoundError:
            pass
    result = pd.concat(merged_dfs_list, ignore_index=True)
    result.sort_values(by="24h")
    top_10_fluctuating = result.head(11)
    coins = top_10_fluctuating["Coin"].to_list()[1:]
    print("Top 10 Coins With Highest Fluctuation in Past 24 Hours: ")
    print("\n".join(coins))


def main(refresh_data=True):
    sbr_connection = ChromiumRemoteConnection(SBR_WEBDRIVER, "goog", "chrome")
    with Remote(sbr_connection, options=ChromeOptions()) as driver:
        driver.get(f"https://www.coingecko.com/?items=300")
        pagination_tab = driver.find_element(By.CLASS_NAME, "gecko-pagination-nav")
        elements = pagination_tab.find_elements(By.TAG_NAME, "span")
        if len(elements) < 3:
            print("Some error occured while scraping")
            return
        num_of_pages_el = elements[-2]
        num_of_pages = int(num_of_pages_el.text)

    if refresh_data:
        with Pool(os.cpu_count()) as p:
            p.map(scrape, range(1, num_of_pages + 1))

    analyse(num_of_pages)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--refresh-data", action="store_true")
    args = parser.parse_args()
    main(args.refresh_data)

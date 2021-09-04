import os
import json
import time
import pickle
from selenium import webdriver
from bs4 import BeautifulSoup
from tqdm import tqdm
from binance import Client
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

#%%

# First, download an executable of your webdriver (Firefox in our case)
# https://github.com/mozilla/geckodriver/releases
geckodriver_path = "./geckodriver"
browser = webdriver.Firefox(executable_path=geckodriver_path)

#%%


def fetch_page_content(page_url):
    browser.get(page_url)
    time.sleep(1)
    browser.execute_script("window.scrollTo(0, 10000)")
    time.sleep(1)
    content = browser.page_source
    return BeautifulSoup(content, "html.parser")


def parse_posts_urls(content):
    posts_urls = content.find("div", class_="u-marginBottom40 js-collectionStream") or content.find("div", class_="u-marginBottom40 js-categoryStream")
    posts_urls = posts_urls.find_all("div")
    posts_urls = (
        div.find("a") for div in posts_urls
        if "js-trackPostPresentation" in div["class"] and div.find("a")
    )
    posts_urls = {
        a.text: a["href"].split("?")[0] for a in posts_urls
    }
    return posts_urls


def parse_date(content):
    for script in content.find("head").find_all("script"):
        if script.contents:
            if "datePublished" in script.contents[0]:
                date_published = json.loads(script.contents[0]).get("datePublished")
                return date_published


def fetch_products_set():
    binance_client = Client(os.environ.get("BINANCE_KEY"), os.environ.get("BINANCE_SECRET"))
    binance_products = binance_client.get_products()["data"]

    products_set = set(
        (prod["b"], prod["an"]) for prod in binance_products
    )
    products_set.update(
        (prod["q"], prod["qn"]) for prod in binance_products
    )
    return products_set


def match_products(products_set, post_text):
    return [
        (symbol, name) for symbol, name in products_set
        if symbol in post_text or name in post_text
    ]


#%%

base_url = "https://blog.coinbase.com/"
soup = fetch_page_content(base_url)
posts_urls = parse_posts_urls(soup)

#%%

menu_urls = [li.find("a")["href"] for li in soup.find_all("li") if "is-external" not in li["class"]]
for url in tqdm(menu_urls):
    soup = fetch_page_content(url)
    posts_urls.update(parse_posts_urls(soup))

#%%

posts_contents = {
    post_title: fetch_page_content(post_url) for post_title, post_url in tqdm(posts_urls.items())
}

#%%

posts_contents = {
    post_title: {
        "url": post_url,
        "content": post_content
    }
    for (post_title, post_url), post_content
    in zip(posts_urls.items(), posts_contents.values())
}

#%%

# Some BS4 cannot be serialized by pickle so it need to be casted to string
posts_backup = {
    post_title: {
        "url": post_details["url"],
        "content": str(post_details["content"])
    }
    for post_title, post_details in posts_contents.items()
}

with open("./posts_contents.pickle", "wb") as file:
    pickle.dump(posts_backup, file)

# with open("./playground/posts_contents.pickle", "rb") as file:
#     posts_backup = pickle.load(file)

#%%

products_set = fetch_products_set()

#%%

for post_title, post_details in posts_contents.items():
    post_details["timestamp"] = parse_date(post_details["content"])
    post_details["text"] = post_details["content"].find("article").text
    post_details["matched_products"] = match_products(products_set, post_details["text"])

#%%

data_by_post = pd.DataFrame([
    {
        "timestamp": post_details["timestamp"],
        "post_title": post_title,
        "post_url": post_details["url"],
        "matched_products_num": len(post_details["matched_products"]),
        "matched_products": post_details["matched_products"]
    }
    for post_title, post_details in posts_contents.items()
])

data_by_post.to_csv("./data_by_post.csv", index=False)

#%%

data_by_symbol = pd.DataFrame([
    {
        "timestamp": post_details["timestamp"],
        "p_symbol": matched[0],
        "p_name": matched[1],
        "post_title": post_title,
        "post_url": post_details["url"],
    }
    for post_title, post_details in posts_contents.items()
    for matched in post_details["matched_products"]
])

data_by_symbol.to_csv("./data_by_symbol.csv", index=False)

#%%


from concurrent.futures import ThreadPoolExecutor, as_completed

#!pip install httpx selectolax pandas openpyxl (use if required in terminal)

import pandas as pd
import httpx
import asyncio
import os
import re
from selectolax.parser import HTMLParser

BASE_URL = "https://www.booking.com"
NO_REVIEWS_URLS = []


csv_file_path = 'NZfinall2.csv'

df = pd.read_csv(csv_file_path)
cities = df.groupby('CITY')

def clean_text(text):
    """
    Clean the text by removing illegal characters for Excel.
    """
    illegal_chars = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')
    return illegal_chars.sub("", text)

def fetch_html(url, headers):
    with httpx.Client() as client:
        response = client.get(url, headers=headers)
        return response.text if response.status_code == 200 else None
def scrape_reviews(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    reviews_data = []
    while True:
        response_text = fetch_html(url, headers)
        if not response_text:
            print(f"Failed to fetch HTML for URL: {url}")
            break

       
        print(f"Processing reviews for URL: {url}")

        tree = HTMLParser(response_text)
        reviews = tree.css('li[itemprop="review"]')

        for review in reviews:
            date_text = review.css_first('p.review_item_date').text().split(': ')[-1].strip()
            date = pd.to_datetime(date_text)

            reviewer_name = review.css_first('p.reviewer_name span[itemprop="name"]').text()
            review_score = float(review.css_first('span.review-score-badge').text().strip())
            review_title = review.css_first('div.review_item_header_content span[itemprop="name"]').text() if review.css_first('div.review_item_header_content span[itemprop="name"]') else ""
            positive_content_nodes = review.css('p.review_pos span[itemprop="reviewBody"]')
            negative_content_nodes = review.css('p.review_neg span[itemprop="reviewBody"]')
            positive_content = ' '.join(node.text() for node in positive_content_nodes)
            negative_content = ' '.join(node.text() for node in negative_content_nodes)
            review_content = positive_content + " " + negative_content
            reviewer_staydate = review.css_first('p.review_staydate').text().replace("Stayed in", "").strip() if review.css_first('p.review_staydate') else ""

            review_data = {
                'review date': date_text,
                'reviewer name': clean_text(reviewer_name),
                'review score': review_score,
                'review title': clean_text(review_title),
                'review content': clean_text(review_content),
                'reviewer staydate': clean_text(reviewer_staydate)
            }
            reviews_data.append(review_data)

        next_page_node = tree.css_first('p.page_link.review_next_page a')
        if not next_page_node:
            print(f"No next page found for URL: {url}")
            break

        next_page = next_page_node.attributes.get('href')
        url = BASE_URL + next_page

    return reviews_data



def scrape_reviews_parallel(booking_url, city, city_folder, hotel_name):
    reviews_data = scrape_reviews(booking_url)

    if reviews_data:  
        hotel_df = pd.DataFrame(reviews_data)
        hotel_xlsx_name = os.path.join(city_folder, hotel_name + '.xlsx')
        hotel_df.to_excel(hotel_xlsx_name, index=False)
    else:
        NO_REVIEWS_URLS.append(booking_url)

    return hotel_name, city


def main():

    l = 0

    tasks = []
    executor = ThreadPoolExecutor(max_workers=61)
    for city, group in cities:
        city_folder = 'result/' + city.replace(" ", "_")
        if not os.path.exists(city_folder):
            os.makedirs(city_folder)


        l += group.size


        for _, row in group.iterrows():
            hotel_name = row['NAME']
            booking_url = BASE_URL + row['REVIEW']
            print(f"Started scraping {hotel_name} in {city}.")

            if (os.path.isfile(f'{city_folder}/{hotel_name}.xlsx')):
                print('Skipping, already exists!')
                continue
            tasks.append((booking_url,city,  city_folder, hotel_name,))

    c = 0
    for future in as_completed((executor.submit(scrape_reviews_parallel, *i) for i in tasks)):
        hotel_name, city = future.result()
        print(f"Scraped! {hotel_name} in {city}. -> {((c/l) * 100):.2f}% ({c}/{l})")
        c+=1


    if NO_REVIEWS_URLS:  
        no_reviews_df = pd.DataFrame(NO_REVIEWS_URLS, columns=["URLs with no reviews"])
        no_reviews_df.to_csv(os.path.join(city_folder, 'no_reviews_urls.csv'), index=False)

main()
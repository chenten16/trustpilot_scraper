import asyncio
from cgi import print_arguments
from math import ceil
import aiohttp
import time
import sys
from bs4 import BeautifulSoup
import json
import csv
from urllib.parse import urljoin, urlparse
import furl
# urls list
reviews = {}


def processResponse(response, url):

    soup = BeautifulSoup(response, "html.parser")
    script_json = soup.find("script", attrs={"id": "__NEXT_DATA__"})
    if script_json == None:
        print("IP is rate limited please try in a while , exiting ...")
        raise Exception("RATE_LIMITED")

    else:
        bulkData = json.loads(script_json.string)
        pageNumber = url.split("page=")[1]
        reviews[pageNumber] = []
        for review in bulkData["props"]["pageProps"]["reviews"]:
            reviews[pageNumber].append(review["text"])


async def get(url, session):
    resp = None
    while resp is None:
        try:
            async with session.get(url=url) as response:
                resp = await response.read()
        except aiohttp.ClientConnectorError as e:
            resp = None
        except aiohttp.ServerDisconnectedError as e:
            resp = None
    if resp != None:
        print(f"Crawling {url}")
        processResponse(resp, url)


def array2csv(arr, filename):
    filename=filename.split('/')[4].split('?')[0]
    wtr = csv.writer(
        open(f"./results/{filename}.csv", "w", encoding="utf-8-sig"),
        delimiter=",",
        lineterminator="\n",
    )
    for x in arr:
        wtr.writerow([x])


async def crawl(urls, reviews_number, website):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(get(url, session)) for url in urls]
        try:
            ret = await asyncio.gather(*tasks)
        except Exception as e:
            for t in tasks:
                t.cancel()

        if(len(reviews)>0):
            sorted_reviews = []
            for k, v in sorted(reviews.items()):
                sorted_reviews.extend(v)
            array2csv(sorted_reviews[:reviews_number], website)
        print("Finished crawling!")
        sys.stdout.flush()
        exit()




def startScript():
    # hi
    taget_website = input("Type the targeted website: ")
    reviews_number = int(input("How many reviews to scrape: "))
    taget_website=furl.furl(taget_website).remove(['page']).url
    pages = [
        furl.furl(taget_website).add(args={'page':number}).url
        for number in range(1, ceil(reviews_number / 20) + 1)
    ]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(crawl(pages, reviews_number, taget_website))
    loop.close()

startScript()

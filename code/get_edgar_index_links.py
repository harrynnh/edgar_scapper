# This script is used to download all the links from edgar index file
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import re
import sqlite3
import datetime
import random
import time

# get the link from this table, no need to pass this to soup as this is relatively clean
def get_indices(start_year, current_year):
    """This function download folder links of edgar filings from the index"""
    current_quarter = (datetime.date.today().month - 1) // 3 + 1
    years = list(range(start_year, current_year))
    quarters = ["QTR1", "QTR2", "QTR3", "QTR4"]
    history = [(y, q) for y in years for q in quarters]
    for i in range(1, current_quarter + 1):
        history.append((current_year, "QTR%d" % i))
    urls = [
        "https://www.sec.gov/Archives/edgar/full-index/%d/%s/crawler.idx" % (x[0], x[1])
        for x in history
    ]
    urls.sort()  # if doesn't work, get the above outside function
    conn = sqlite3.connect("data/edgar_index.db")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS edgar_index")
    cur.execute(
        "CREATE TABLE edgar_index (comn TEXT, type TEXT, cik TEXT, date TEXT, path TEXT)"
    )
    for url in urls:
        try:
            content = requests.get(url)
            lines = content.text.splitlines()
        except requests.exceptions.HTTPError:
            return None
        try:
            nameloc = lines[7].find("Company Name")
            typeloc = lines[7].find("Form Type")
            cikloc = lines[7].find("CIK")
            dateloc = lines[7].find("Date Filed")
            urlloc = lines[7].find("URL")
        except AttributeError:
            return None
        records = [
            tuple(
                [
                    line[:typeloc].strip(),
                    line[typeloc:cikloc].strip(),
                    line[cikloc:dateloc].strip(),
                    line[dateloc:urlloc].strip(),
                    line[urlloc:].strip(),
                ]
            )
            for line in lines[9:]
        ]
        cur.executemany("INSERT INTO edgar_index VALUES (?, ?, ?, ?, ?)", records)
        print(url, "downloaded and wrote to SQL")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    get_indices(1993, 2021)

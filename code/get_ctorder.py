import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import re
import sqlite3
import random
import time

cwd = os.chdir("/Users/harrynnh/Dropbox/workspace/webscraping/")

# Load index links
conn = sqlite3.connect(
    "/Users/harrynnh/Dropbox/workspace/webscraping/data/edgarIndex.db"
)
cur = conn.cursor()
query = "SELECT * FROM edgarIndex"  # ideally select cols, but should without specific specfications but contains 10-K
# cur.execute(query)
# result = cur.fetchall()
edgar_index = pd.read_sql(query, conn)
conn.close()

edgar_ctorder_index = edgar_index.loc[
    edgar_index.type.str.contains("CT ORDER"),
]
paths = edgar_ctorder_index["path"].tolist()

################################################################################
# FUNCTION TO DOWNLOAD THE LINKS OF CT ORDER
################################################################################
def getReportLinks(urlList):
    heads = {
        "User-Agent": "harry.nnh@gmail.com",
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    }
    conn = sqlite3.connect("data/edgar_ctorder_links.db")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS edgar_ctorder_links")
    cur.execute(
        "CREATE TABLE edgar_ctorder_links (cName TEXT, cik TEXT, filingDate TEXT, accepted TEXT, formText TEXT, formLink TEXT, txtLink TEXT)"
    )
    for url in urlList:
        time.sleep(0.1)
        cName = ""
        cik = ""
        filingDate = ""
        accepted = ""
        formText = ""
        formLink = ""
        txtLink = ""
        try:
            html = requests.get(url, timeout=30, headers=heads)
            soup = BeautifulSoup(html.content, "lxml")
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.SSLError,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as e:  # here I set 30 secs timeout and moveon if I can't get link
            pass
        try:
            cInfo = soup.find("span", {"class": "companyName"}).text.strip().split("\n")
            cName = cInfo[0].strip()
            cik = cInfo[1].strip()[5:15]
            tableFile = soup.find("table", {"class": "tableFile"})
            filingInfo = soup.find_all("div", {"class": "formGrouping"})
        except AttributeError as e:
            pass
        for info in filingInfo:
            for elem in info:
                try:
                    if "Filing Date" in elem.text.strip():
                        filingDate = elem.find_next_sibling("div").text.strip()
                    if "Accepted" in elem.text.strip():
                        accepted = elem.find_next_sibling("div").text.strip()
                except AttributeError:
                    pass
        try:
            link = tableFile.find_all("a")  # pdf in the first link
            formLink = "https://www.sec.gov" + link[0].get("href")
            txtLink = "https://www.sec.gov" + link[-1].get("href")  # txt in the last
        except AttributeError:
            pass
        cur.execute(
            "INSERT INTO edgar_ctorder_links VALUES (?, ?, ?, ?, ?, ?, ?)",
            (cName, cik, filingDate, accepted, formText, formLink, txtLink),
        )
        print(formLink, txtLink, accepted, "wrote to DB")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    getReportLinks(paths)

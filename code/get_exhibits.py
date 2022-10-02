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

################################################################################
# READ DATA (paths)
################################################################################
# edgar_10k_index = pd.read_csv("./data/10k_index_links.csv")
# edgar_10q_index = pd.read_csv("./data/10q_index_links.csv")
# paths_10k = edgar_10k_index["path"].tolist()
# paths_10q = edgar_10q_index["path"].tolist()
################################################################################
# FUNCTION TO DOWNLOAD LINKS IN THE INDEX FOLDER
################################################################################


def getReportLinks(urlList, file_type):
    heads = {
        "User-Agent": "harry.nnh@gmail.com",
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov",
    }
    conn = sqlite3.connect(f"data/edgar_{file_type}_links.db")
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS edgar_{file_type}_links")
    cur.execute(
        f"CREATE TABLE edgar_{file_type}_links (cName TEXT, cik TEXT, filingDate TEXT, accepted TEXT, formText TEXT, formLink TEXT, exh101 TEXT, exh102 TEXT, exh103 TEXT, exh104 TEXT, exh105 TEXT, exh106 TEXT, exh107 TEXT, exh108 TEXT, exh109 TEXT, exh1010 TEXT, exh1011 TEXT, exh1012 TEXT, exh1013 TEXT, exh1014 TEXT, exh1015 TEXT, exh1016 TEXT, exh1017 TEXT, exh1018 TEXT, exh1019 TEXT, exh1020 TEXT, txtLink TEXT)"
    )
    for url in urlList:
        time.sleep(0.1)
        cName = "NA"
        cik = "NA"
        filingDate = "NA"
        accepted = "NA"
        formText = "NA"
        formLink = "NA"
        txtLink = "NA"
        ex10 = [None] * 20  # increase if more than 10 exhibits
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
            var_holder = {}
            rows = tableFile.find_all("tr")
            n = 0
            for row in rows[1:]:
                row = row.find_all("td", {"scope": "row"})
                if "EX-10" in row[3].text.strip():
                    n += 1
                    file_type = row[3].text.strip()
                    link_ex = row[2].find("a").get("href")
                    ex10.insert(n - 1, "https://www.sec.gov" + link_ex)
                if (".htm" in row[2].text.strip()) and ("10-Q" in row[3].text.strip()):
                    link_10 = row[2].find("a").get("href")
                    formLink = "https://www.sec.gov" + link_10

            txtLink = "https://www.sec.gov" + tableFile.find_all("a")[-1].get(
                "href"
            )  # txt in the last
        except AttributeError:
            pass
        records = (
            cName,
            cik,
            filingDate,
            accepted,
            formText,
            formLink,
            ex10[0],
            ex10[1],
            ex10[2],
            ex10[3],
            ex10[4],
            ex10[5],
            ex10[6],
            ex10[7],
            ex10[8],
            ex10[9],
            ex10[10],
            ex10[11],
            ex10[12],
            ex10[13],
            ex10[14],
            ex10[15],
            ex10[16],
            ex10[17],
            ex10[18],
            ex10[19],
            txtLink,
        )  # I'm brute forcing this for now.
        cur.execute(
            "INSERT INTO edgar_10q_links VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            records,
        )
        print(cik, filingDate, "wrote to DB")
    conn.commit()
    conn.close()


################################################################################
# DOWNLOAD
################################################################################
paths_10q = pd.read_csv("./data/10q_index_links.csv")["path"].sample(10).to_list()
if __name__ == "__main__":
    getReportLinks(paths_10q, "10q")

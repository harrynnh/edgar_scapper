# This code is to download html files from EDGAR
import requests
import re
import pandas as pd

################################################################################

# Download htmls
def download_html(urls):
    for url in urls:
        name = re.search(r"data/(.*?).htm", url).group(1).replace("/", "-")
        with open(f"../output/{name}.html", "wb") as f:
            res = requests.get(url, headers=headers)
            f.write(res.content)
            print(res.ok, name, "downloaded")


################################################################################
# DOWNLOAD HTML
################################################################################

if __name__ == "__main__":
    exhibit_link = pd.read_csv(input("Enter path to list urls: "))
    download_html(exhibit_link)

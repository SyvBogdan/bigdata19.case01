"""
Assignment 02
=============

The goal of this assignment is to implement synchronous scraping using standard python modules,
and compare the scraping speed to asynchronous mode.

Run this code with

    > fab run assignment02.py
"""

from yahoo import read_symbols, YAHOO_HTMLS

import time
import progressbar
import urllib

def create_request(symbol):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36', }
    return urllib.request.Request(url=f'https://finance.yahoo.com/quote/{symbol}/profile?p={symbol}', headers=headers)


def send_request(req_param):
    with urllib.request.urlopen(
            create_request(req_param)) as response:
        return response.read()


def write_result(name, body):
    with open(YAHOO_HTMLS / f'{name}.html', "w+", encoding="utf-8") as file:
        file.write(body.decode('utf-8'))


def scrape_descriptions_sync():
    symbols = read_symbols()

    i = 1
    with progressbar.ProgressBar(max_value=len(symbols)) as bar:
        for smb in symbols:
            content = send_request(smb)
            write_result(smb, content)
            bar.update(i)
            i += 1


def main():
    scrape_descriptions_sync()


if __name__ == '__main__':
    main()


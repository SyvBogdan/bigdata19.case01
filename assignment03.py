"""
Assignment 03
=============

The goal of this assignment is to start working on individual project.
You need to find data source, and scrape it to Parquet file.
It is recommended to scrape data asynchronously, in batches.

Run this code with

    > fab run assignment03:scrape_data()
"""
import asyncio
import sys
from collections import defaultdict

import aiofiles
from aiohttp import ClientSession

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

import config as cfg

DATA_FILE = cfg.BUILDDIR / 'data.parquet'

#data for test only!!!!
def read_stock_indexes():
    return ["AAOI", "AAPL", "AFL", "ALK", "ALLE"]


def scrape_batch_async(batch):
    dictionary = dict()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15',
    }

    async def fetch(stock_index, session, dictionary):
        async with session.get(f'https://www.marketwatch.com/investing/stock/{stock_index}') as response:
            text = await response.read()
            dictionary[stock_index] = text

    async def run(symbols, dictionary):
        async with ClientSession(headers=headers) as session:
            tasks = (asyncio.ensure_future(fetch(symbol, session, dictionary)) for symbol in symbols)
            await asyncio.gather(*tasks)

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda x, y: None)
    loop.run_until_complete(asyncio.ensure_future(run(batch, dictionary)))

    return dictionary


def write_batch_to_parquet(next_dictionary, compression_type, progress):
    batch = defaultdict(list)

    names = ('symbol', 'html')

    for stock, html in next_dictionary.items():
        batch["stock"].append(stock)
        batch["html"].append(html)
        progress.update(1)

    pg_table = pa.Table.from_arrays([pa.array(batch[n]) for n in names], names)

    writer = pq.ParquetWriter(DATA_FILE, pg_table.schema, use_dictionary=False, compression=compression_type,
                              flavor={'spark'})
    writer.write_table(pg_table)
    writer.close()


def scrape_data():
    batch_size = 5
    compression_type = "BROTLI"

    all_stocks = read_stock_indexes()

    progress = tqdm(total=len(all_stocks), file=sys.stdout, disable=False)

    def read_next_batch(batch_size):

        batch = list()

        for next_stock in all_stocks:
            batch.append(next_stock)
            if len(batch) >= batch_size:
                yield batch
                batch = list()
        if batch:
            yield batch

    for batch in read_next_batch(batch_size):
        dt = scrape_batch_async(batch)
        write_batch_to_parquet(dt, compression_type, progress)


def main():
    scrape_data()


if __name__ == '__main__':
    main()

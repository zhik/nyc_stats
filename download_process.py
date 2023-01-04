#!/usr/bin/env python
# coding: utf-8

import pandas as pd 
import urllib.request
from zipfile import ZipFile
from pathlib import Path
import asyncio
from playwright.async_api import async_playwright

#add headers
opener = urllib.request.build_opener()
opener.addheaders = [('User-agent', 'Mozilla/5.0')]
urllib.request.install_opener(opener)

#create temp folder
temp_path = Path("./temp/")
temp_path.mkdir(parents=True, exist_ok=True)

#datasets and their methods
async def convertFromXls(url, filename, oargs):
    #download with xls (the proper extension)
    path = temp_path / filename.replace('csv','xls')
    urllib.request.urlretrieve(url, path)
    df = pd.read_excel(path, skiprows = oargs.get('skiprows'),
                             sheet_name = oargs.get('sheet_name',0))
    df.to_csv(filename, index = False)

async def convertFromXlsx(url, filename, oargs):
    #download with xlsx (the proper extension)
    path = temp_path / filename.replace('csv','xlsx')
    urllib.request.urlretrieve(url, path)
    df = pd.read_excel(path, skiprows = oargs.get('skiprows'),
                             sheet_name = oargs.get('sheet_name',0))
    df.to_csv(filename, index = False)
    
async def extractFromZip(url, filename, oargs):
    path = temp_path / filename.replace('csv','zip')
    urllib.request.urlretrieve(url, path)
    #extract to main folder
    with ZipFile(path,"r") as zip_ref:
        zip_ref.extract(filename, './')
    
async def downloadOpenTable(url, filename, oargs):
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        page = await browser.new_page()
        await page.goto(url)
        await page.title()
        async with page.expect_download() as download_info:
            button = page.get_by_text("Download dataset").first
            await button.click()
            await page.wait_for_timeout(1000)
        download = await download_info.value
        await download.save_as(filename)
        await browser.close()
        df = pd.read_csv(filename)
        #filter out
        df[df['Name'].isin([
            'New York','Chicago','Boston','Los Angeles'])].to_csv(filename, index = False)  

datsets = [
    #todo clean up datasets using sheet selections
    {
        'url': 'https://dol.ny.gov/statistics-new-york-city-employment-statistics',
        'filename': 'nychist.csv',
        'method': convertFromXlsx,
        'skiprows': 1
    },
    {
        'url': 'https://dol.ny.gov/statistics-new-york-city-labor-force-data',
        'filename': 'nyclfsa.csv',
        'method': convertFromXlsx,
        'skiprows': 2
    },
    {
        'url': 'https://dol.ny.gov/statistics-laussaxls',
        'filename': 'laus_sa_state.csv',
        'method': convertFromXls,
        'skiprows': 2,
        'sheet_name': 0
    },
    {
        'url': 'https://dol.ny.gov/statistics-laussaxls',
        'filename': 'laus_sa_city.csv',
        'method': convertFromXls,
        'skiprows': 2,
        'sheet_name': 1
    },
    {
        'url': 'https://dol.ny.gov/statistics-state-and-area-employment-hours-and-earnings',
        'filename': 'nyc-weekly-earnings.csv',
        'method': convertFromXlsx,
        'skiprows': 12,
        'sheet_name': 2
    },
    {
        'url': 'https://cdn-charts.streeteasy.com/rentals/All/medianAskingRent_All.zip',
        'filename': 'medianAskingRent_All.csv',
        'method': extractFromZip
    },
    {
        'url': 'https://www.opentable.com/state-of-industry',
        'filename': '2020-2022vs2019_Reopened_Seated_Diner_Data.csv',
        'method': downloadOpenTable
    },
]



async def main():
    for dataset in datsets:
        await dataset['method'](dataset['url'], dataset['filename'], dataset)
        print(f"downloaded {dataset['filename']}")

asyncio.run(main())






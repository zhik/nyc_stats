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

        data = await page.evaluate("""() => {
                const seatedDiners = window.__INITIAL_STATE__ .stateOfTheIndustry.seatedDiners
                const dates = seatedDiners.dailyHeaders.map(d => d.replaceAll('-','/')) 
                const cities = [ 'New York','Chicago','Boston','Los Angeles']
                const cityData = seatedDiners.cities.filter(d => cities.includes(d.name)).map(city => {
                    const c = {Name: city.name, Type: 'city', id: city.name + '-city'}
                    city.dailyYoY.forEach((d,i) => c[dates[i]] = d)
                    return c
                })

                const stateData = seatedDiners.states.filter(d => d.name === 'New York').map(state => {
                    const s = {Name: state.name, Type: 'state',id: state.name + '-state'}
                    state.dailyYoY.forEach((d,i) => s[dates[i]] = d)
                    return s
                })

                return [...cityData, ...stateData]
            }
        """)  

        df = pd.DataFrame(data)
        #filter out columns for the current year, then join
        df_legacy = pd.read_csv('./2020-2022vs2019_Reopened_Seated_Diner_Data_Legacy.csv')
        df_legacy['id'] = df_legacy.apply(lambda d: d['Name'] + '-' + d['Type'], axis = 1)
        del df_legacy['Name']
        del df_legacy['Type']
        columns_before_2023 = [col for col in df_legacy.columns if '2023' not in col] 
        pd.merge(df_legacy[columns_before_2023], df, on = 'id', how = 'outer').to_csv(filename, index = False)


datsets = [
    #todo clean up datasets using sheet selections
    {
        'url': 'https://dol.ny.gov/statistics-new-york-city-employment-statistics',
        'filename': 'nychist.csv',
        'method': convertFromXlsx,
        'skiprows': 0
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
    }
]



async def main():
    for dataset in datsets:
        await dataset['method'](dataset['url'], dataset['filename'], dataset)
        print(f"downloaded {dataset['filename']}")

asyncio.run(main())






import requests
import pandas as pd
import numpy as np
import threading
from functools import reduce
import warnings
import sqlite3


class CatalogExtractor():

    def __init__(self, series_codes):
        self.series = series_codes
    
    def extractor(self, catalog_series_file):
        df = pd.read_excel(catalog_series_file)
        df['mask'] = np.where(df.loc[:, 'Código'].isin(self.series), 1, 0)
        df = df[df['mask']==1]
        df.sort_values(by='Código', inplace=True)
        df.drop_duplicates(subset='Código', inplace=True)
        df.columns.str.strip()
        df['Serie'] = df.loc[:, 'Nombre cuadro'].str.strip() + '-' + df.loc[:,'Nombre de la serie'].str.strip()                           
        return dict(zip(df.loc[:,'Serie'], df.loc[:,'Código']))


class BankScrapper():
    
    def __init__(self, user, password, firstdate, lastdate, series_dict):
        self.user = user
        self.password = password
        self.firstdate = firstdate
        self.lastdate = lastdate
        self.series_name = series_dict.keys()
        self.series_values = series_dict.values()
        self.dataframes = []
    
    def get_data(self, serie):
        url = 'https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx?user={}&pass={}'\
            '&firstdate={}&lastdate={}&timeseries={}&function=GetSeries'.format(self.user, self.password, self.firstdate, self.lastdate, serie)
        response = requests.get(url)
        response = response.json()
        series_name = list(self.series_name)[list(self.series_values).index(serie)]
        response = response["Series"]["Obs"]
        df_scraped = pd.DataFrame(response)
        df_scraped.rename({'value': '{}'.format(series_name)}, axis='columns', inplace=True)
        self.dataframes.append(df_scraped)
        return self.dataframes 


def main():  
    series_codes = [
    'F032.IMC.IND.Z.Z.EP13.Z.Z.0.M',
    'F074.IPC.VAR.Z.Z.C.M',
    'F019.IPC.V12.10.M',
    'F019.PPB.PRE.100.D',
    'F073.TCO.PRE.Z.D',
    'F049.DES.TAS.INE9.10.M',
    'F049.DES.TAS.INE9.26.M',
    'F049.DES.TAS.INE9.12.M',]       
    
    e = CatalogExtractor(series_codes=series_codes)
    scrapper = BankScrapper(user='xxxxxx', password='xxxxx', firstdate='2014-01-31', lastdate='2022-01-31',
                            series_dict=e.extractor(catalog_series_file='series.xlsx'))
    threads = []
    for i in e.series:
        thread = threading.Thread(target=scrapper.get_data, args=[i])
        thread.start()
        threads.append(thread) 

    for thread in threads:
        thread.join()   

    warnings.filterwarnings('ignore')
    data_scraped = reduce(lambda  left,right: pd.merge(left,right,on=['indexDateString'], how='outer'), scrapper.dataframes)
    data_scraped = data_scraped[data_scraped.columns.drop(list(data_scraped.filter(regex='statusCode.*')))]        
    data = data_scraped.copy(deep=True) #con sql no sería necesario
    
    con = sqlite3.connect('indicators.sqlite')
    data.to_sql('scrapped', con, if_exists='replace')
    con.close()

if __name__=='__main__':
    main()
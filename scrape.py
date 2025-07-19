import pandas as pd
import numpy as np

from datetime import datetime
from get_data_mulithreaded import get_data_multithreaded
from get_offers import get_offers


#gets offers
lista_ofert = get_offers(pages=120)


#gets data for the offers
data = get_data_multithreaded(lista_ofert=lista_ofert)


#saves the data
timestamp = datetime.now().strftime('%Y_%m_%d')
nazwa_pliku = f'dane_{timestamp}.csv'
data.to_csv(nazwa_pliku, index= False)
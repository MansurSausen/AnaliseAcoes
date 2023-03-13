import pandas as pd
import wget
import zipfile as zip
import yfinance as yf
import numpy as np
import datetime
#from zipfile import ZipFIle


dre = pd.read_csv('DADOS//itr_dfp_cia_aberta__cia_aberta_DRE_ind_2011_2022.csv')
dre = dre[dre['ORDEM_EXERC'] == "ÚLTIMO"]
#empresas = dre[['DENOM_CIA','CD_CVM']].drop_duplicates().set_index('CD_CVM')


empresas = pd.read_csv('DADOS//IBOV CVM CODES.csv', sep = ';',decimal = ',', encoding='ISO-8859-1')
empresas  = empresas.head(5)
empresas = empresas.reset_index()


        # Seleciona a empresa
        # AGRO3 20036   CBA 25984   PETR4  9512  BBAS3 1023 ITUB4 19348  suzano 13986  cielo 21733
empresa = dre[dre['CD_CVM'] == 21610]
contas = empresa[['CD_CONTA', 'DS_CONTA']].drop_duplicates().set_index('CD_CONTA')

#with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
#    print(contas)
    
ticker = 'ABEV3.SA'
ticker_yahoo = yf.Ticker(ticker)
data = ticker_yahoo.history()
last_quote = data['Close'].iloc[-1]
print(ticker, last_quote)


resultadoFinal = {
    'Empresa':[],
    'Preco' :[],
    'Valuation':[],
    'Diferenca':[],
    'Diferenca_Percentual':[]
              }

dfResultadoFinal = pd.DataFrame(resultadoFinal)
print(dfResultadoFinal)
new_row = {'Empresa':ticker, 'Preco':last_quote, 'Valuation':last_quote, 'Diferenca':last_quote, 'Diferenca_Percentual':last_quote}
#append row to the dataframe
dfResultadoFinal = dfResultadoFinal.append(new_row, ignore_index=True)

print(dfResultadoFinal)

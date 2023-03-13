import pandas as pd
import wget
#import zipfile as zip
from zipfile import ZipFile

#processo de download
url_base = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'


arquivos_zip = []

for ano in range(2011,2023):
    arquivos_zip.append(f'dfp_cia_aberta_{ano}.zip')

for arq in arquivos_zip:
    wget.download(url_base+arq)


url_baseITR = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/'

arquivos_zip_itr = []

for ano in range(2011,2023):
    arquivos_zip_itr.append(f'itr_cia_aberta_{ano}.zip')

for arq in arquivos_zip_itr:
    wget.download(url_baseITR+arq)


for arq in arquivos_zip:
    ZipFile(arq, 'r').extractall('CVM')

for arq in arquivos_zip_itr:
    ZipFile(arq, 'r').extractall('CVM')


#processo de uniao, 
#print(arquivos_zip)

#nomes = ['BPA_con','BPA_ind','BPP_con','BPP_ind','DFC_MD_con','DFC_MD_ind','DFC_MI_con','DFC_MI_ind','DMPL_con' ,'DMPL_ind', 'DRA_con', 'DRA_ind', 'DRE_con', 'DRE_ind', 'DVA_con', 'DVA_ind']
#nomes = ['itr_cia_aberta_BPA_con','itr_cia_aberta_BPA_ind','itr_cia_aberta_BPP_con','itr_cia_aberta_BPP_ind','itr_cia_aberta_DFC_MD_con','itr_cia_aberta_DFC_MD_ind','itr_cia_aberta_DFC_MI_con','itr_cia_aberta_DFC_MI_ind','itr_cia_aberta_DMPL_con' ,'itr_cia_aberta_DMPL_ind', 'itr_cia_aberta_DRA_con', 'itr_cia_aberta_DRA_ind', 'itr_cia_aberta_DRE_con', 'itr_cia_aberta_DRE_ind', 'itr_cia_aberta_DVA_con', 'itr_cia_aberta_DVA_ind' ,'dfp_cia_aberta_BPA_con','dfp_cia_aberta_BPA_ind','dfp_cia_aberta_BPP_con','dfp_cia_aberta_BPP_ind','dfp_cia_aberta_DFC_MD_con','dfp_cia_aberta_DFC_MD_ind','dfp_cia_aberta_DFC_MI_con','dfp_cia_aberta_DFC_MI_ind','dfp_cia_aberta_DMPL_con' ,'dfp_cia_aberta_DMPL_ind', 'dfp_cia_aberta_DRA_con', 'dfp_cia_aberta_DRA_ind', 'dfp_cia_aberta_DRE_con', 'dfp_cia_aberta_DRE_ind', 'dfp_cia_aberta_DVA_con', 'dfp_cia_aberta_DVA_ind']
nomes = ['_cia_aberta_BPA_con','_cia_aberta_BPA_ind','_cia_aberta_BPP_con','_cia_aberta_BPP_ind','_cia_aberta_DFC_MD_con','_cia_aberta_DFC_MD_ind','_cia_aberta_DFC_MI_con','_cia_aberta_DFC_MI_ind','_cia_aberta_DMPL_con' ,'_cia_aberta_DMPL_ind', '_cia_aberta_DRA_con', '_cia_aberta_DRA_ind', '_cia_aberta_DRE_con', '_cia_aberta_DRE_ind', '_cia_aberta_DVA_con', '_cia_aberta_DVA_ind']
#pd.read_csv('CVM\\itr_cia_aberta_2011.csv', sep = ';',decimal = ',', encoding='ISO-8859-1')
 #Tem erro ainda, esta criando separado ITR e DFP
for nome in nomes:
    arquivo = pd.DataFrame()
    for ano in range(2011,2023):
        for tipo in ['itr','dfp']:
            arquivo = pd.concat([arquivo, pd.read_csv(f'CVM\\{tipo}{nome}_{ano}.csv', sep = ';',decimal = ',', encoding='ISO-8859-1')])
        #arquivo = pd.concat([arquivo, pd.read_csv(f'src\CVM\\itr_cia_aberta_{nome}_{ano}.csv', sep = ';',decimal = ',', encoding='ISO-8859-1')])
    arquivo.to_csv(f'DADOS\\itr_dfp_cia_aberta_{nome}_2011_2022.csv', index = False)





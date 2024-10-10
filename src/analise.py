import pandas as pd
import wget
import zipfile as zip
import yfinance as yf
import numpy as np
import datetime
#from pyspark.sql import SparkSession
#from zipfile import ZipFIle
#spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#empresas = dre[['DENOM_CIA','CD_CVM']].drop_duplicates().set_index('CD_CVM')

def getValorPresenteTopEmpresasIbov(top, anoBase,dre):
    #dre = pd.read_csv('DADOS//itr_dfp_cia_aberta__cia_aberta_DRE_ind_2011_2022.csv')
    #dre = dre[dre['ORDEM_EXERC'] == "ÚLTIMO"]

    empresas = pd.read_csv('DADOS//IBOV CVM CODES.csv', sep = ';',decimal = ',', encoding='ISO-8859-1')
    empresas  = empresas.head(top)
    empresas = empresas.reset_index()

    resultadoFinal = {
        'Empresa':[],
        'Preco' :[],
        'Valuation':[],
        'Diferenca':[],
        'Diferenca%':[],
        'M LPA':[],
        'M G':[],
                }

    dfResultadoFinal = pd.DataFrame(resultadoFinal)

    for index, row in empresas.iterrows():
        try:
            print(row['CVM'])
            LPATry = getEmpresaLPA(row['CVM'],anoBase,dre)
            LPA = LPATry
        except:
            print("An exception occurred")
            new_row = {'Empresa':row['AS'], 'Preco':0, 'Valuation':0, 'Diferenca':0, 'Diferenca%':0}
            #append row to the dataframe
            dfResultadoFinal = dfResultadoFinal.append(new_row, ignore_index=True)
            continue
        #pra q?
        LPA = getEmpresaLPA(row['CVM'],anoBase,dre)
        LPA = adicionaG(LPA)
        mediaLPA = LPA['VL_CONTA'].mean()
        mediaG = LPA['G'].mean()
        printMedias(LPA)
        valorIntrinseco = getValorIntrinseco(mediaG, mediaLPA)
        dfResultadoFinal = adicionaResultadoFinal(dfResultadoFinal,row['AS'], valorIntrinseco,mediaG, mediaLPA)
        #ticker = row['AS']

        # Seleciona a empresa
        # AGRO3 20036   CBA 25984   PETR4  9512  BBAS3 1023 ITUB4 19348  suzano 13986  cielo 21733
        #dfResultadoFinal.index = dfResultadoFinal['Diferenca%']

    dfResultadoFinal = dfResultadoFinal.sort_values(by=['Diferenca%'], ascending=False)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(dfResultadoFinal)


def getCodigoCVM(codigoEmpresa):
    empresas = pd.read_csv('DADOS//IBOV CVM CODES.csv', sep = ';',decimal = ',', encoding='ISO-8859-1')
    empresas.reset_index()
    empresa = empresas[empresas['AS'] == codigoEmpresa]
    return empresa['CVM']


def printContas(codCVM, ano,dre):
    empresa = dre[dre['CD_CVM'] == codCVM]
    contas = empresa[['CD_CONTA', 'DS_CONTA'].drop_duplicates().set_index('CD_CONTA')]
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(contas)

#retorna o lucro por acao da empresa, se baseando no DRE
def getEmpresaLPA(codCVM, ano,dre):
    empresa = dre[dre['CD_CVM'] == codCVM]
    #contas = empresa[['CD_CONTA', 'DS_CONTA'].drop_duplicates().set_index('CD_CONTA')

    # Seleciona qual conta realizar calculo
    #ALTERAR somar os valores e nao só verificar 1, entender o que é esse código
    LPA = empresa[empresa['CD_CONTA'] == '3.99.01.01']
    if (LPA.size > 0):
        print("Maior11")
    else:
        LPA = empresa[empresa['CD_CONTA'] == '3.99.01.02']
        if (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
            print("Maior12")
        else:
            LPA = empresa[empresa['CD_CONTA'] == '3.99.02.01']
            if (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
                print("Maior21")
            else:
                LPA = empresa[empresa['CD_CONTA'] == '3.99.02.02']
                if (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
                    print("Maior22")
                else:
                    LPA = empresa[empresa['CD_CONTA'] == '3.99']
                    print(LPA)
                    if (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
                        print("Maior22")
                    else:
                        LPA = empresa[empresa['CD_CONTA'] == '3.99.01']
                        if (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
                            print("Maior9901")
                        else:
                            LPA = empresa[empresa['CD_CONTA'] == '3.99.02']
                            print("Maior9902")
                            if not (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
                                print("Erro")
                                raise Exception("No VPL")

    LPA["DT_REFER"] = pd.to_datetime(LPA['DT_REFER'], format="%Y-%m-%d")
    LPA['Ano'] = LPA['DT_REFER'].apply(lambda time: time.year)
            # LPA['Mes'] = LPA['DT_REFER'].apply(lambda time:time.month)
    filter = LPA["Ano"] > ano  # seleciona a partir de qual data
            #filter = LPA["DT_REFER"] > pd.to_datetime("2016-01-01", format="%Y-%m-%d")  # seleciona a partir de qual data
    LPA = LPA.where(filter)
    
    LPA = LPA.sort_values(by=['DT_REFER'])
    LPA = LPA[["DT_REFER","VL_CONTA","Ano"]]
    LPA = LPA.dropna()
    print(LPA)
    return LPA

            # tratamento erro de valor em milhares
            #LPA['VL_CONTA'] = LPA['VL_CONTA'].apply(lambda x: x/1000 if x > 100 else x)

            #LPA.index = LPA['DT_REFER'] serve para dar join
            # indicadores = prices.join(LPA['VL_CONTA'],how='outer')
            # LPA.fillna(method = 'ffill', inplace = True)

def mediaLpa(a, b):
    if a< 0.1:
        return (a+b)/2
    else:
        return a
    
   #calculo da media de crescimento, utilizando a média móvel de 4 
def adicionaG(LPA):
    # calcular media movel do LPA, pois ele oscila mt nos anos
    LPA['LPAMedia4'] = LPA["VL_CONTA"].rolling(4).mean()

    # calcula G do crescimento da media movel de LPA = Valor final/ Valor inicial
    #filter = LPA["LPAMedia4"] > 0.1  
    #LPA = LPA.where(filter)
    #LPA[['VL_CONTA','LPAMedia4']] =LPA[['VL_CONTA','LPAMedia4']].apply(lambda x: mediaLpa(x,x.shift(1)))
    #rdd=spark.sparkContext.parallelize(LPA['LPAMedia4'])
    #rdd2=rdd.reduceByKey(lambda x: (x[0] + x[1])/2 if x[0] < 0.1 else x[0])
    #print(rdd2)
    #LPA['LPAMedia4'] = LPA['LPAMedia4'].apply(lambda x: (x.shift(1) + x)/2 if x < 0.1 else x)

    LPA['G'] = LPA['LPAMedia4'].pct_change()
    filter = LPA["G"] < 15  
    LPA = LPA.where(filter)

    # Media nao sei se é o melhor, talvez seria melhor comparar atual/ 5 anos antes ^ 1/5
    return LPA

def printMedias(LPA):
    mediaLPA = LPA['VL_CONTA'].mean()
    mediaG = LPA['G'].tail(35).mean()
    print(LPA.tail(35)[['VL_CONTA', 'LPAMedia4', 'G']])
    print("Valor media LPA: %.2f" % round(mediaLPA, 2))
    print("Valor media G: %.2f" % round(mediaG, 2))
    print(mediaLPA)
    print(mediaG)

def getLpa5(mediaG,mediaLPA):
        # calcular LPA estimativa proximos 5 anos
    LPAFuturo1 = mediaLPA * (1+mediaG)
    LPAFuturo2 = LPAFuturo1 * (1+mediaG)
    LPAFuturo3 = LPAFuturo2 * (1+mediaG)
    LPAFuturo4 = LPAFuturo3 * (1+mediaG)
    LPAFuturo5 = LPAFuturo4 * (1+mediaG)
    return LPAFuturo5

def getLpaFuturoNoPresente(mediaG,mediaLPA):
    # calcular LPA estimativa proximos 5 anos
    LPAFuturo1 = mediaLPA * (1+mediaG)
    LPAFuturo2 = LPAFuturo1 * (1+mediaG)
    LPAFuturo3 = LPAFuturo2 * (1+mediaG)
    LPAFuturo4 = LPAFuturo3 * (1+mediaG)
    LPAFuturo5 = LPAFuturo4 * (1+mediaG)
    print("Valor LPA1: %.2f" % round(LPAFuturo1, 2))
    somaLPAFuturo = LPAFuturo1+LPAFuturo2+LPAFuturo3+LPAFuturo4+LPAFuturo5
    print("Valor Soma LPA Futuro: %.2f" % round(somaLPAFuturo, 2))
    return somaLPAFuturo
#FALTA TRAZER A VALOR PRESENTE O LPA


def getValorPerpetuidadeAtual(LPAFuturo5):
    Ke = 0.15
    Gperpetuidade = 0.065  # Inflacao+1 * PIB+1   - 1
    perpetuidade = LPAFuturo5*(1+Gperpetuidade)/(Ke - Gperpetuidade)
    print("Valor Perpetuidade: %.2f" % round(perpetuidade, 2))

    perpetuidadeAtual = perpetuidade / ((1+Ke)**5)
    print("Valor Perpetuidade Atual: %.2f" % round(perpetuidadeAtual, 2))
    return perpetuidadeAtual

def getValorIntrinseco(mediaG, mediaLPA):
    somaLPAFuturo = getLpaFuturoNoPresente(mediaG,mediaLPA)
    perpetuidadeAtual = getValorPerpetuidadeAtual(getLpa5(mediaG,mediaLPA))
    valorIntrinseco = somaLPAFuturo+perpetuidadeAtual
    print("Valor Intrinseco: %.2f" % round(valorIntrinseco, 2))
    return valorIntrinseco


def adicionaResultadoFinal(dfResultadoFinal,tickerName, valorIntrinseco,mediaG, mediaLPA):
    #ticker = row['AS']
    ticker_yahoo = yf.Ticker(tickerName)
    data = ticker_yahoo.history()
    last_quote = data['Close'].iloc[-1]
    print(tickerName, last_quote)
    diferenca = valorIntrinseco - last_quote
    percentualDiferenca = diferenca / last_quote
    print(diferenca)
    print(percentualDiferenca)

    new_row = {'Empresa':tickerName, 'Preco':round(last_quote, 3), 'Valuation':round(valorIntrinseco, 3), 'Diferenca':round(diferenca, 2), 'Diferenca%':round(percentualDiferenca, 3), 'M LPA':round(mediaLPA, 3), 'M G':round(mediaG, 3)}
    #append row to the dataframe
    dfResultadoFinal = dfResultadoFinal.append(new_row, ignore_index=True)
    return dfResultadoFinal

def analiseIndividual(codigo,ano):
    LPA = getEmpresaLPA(codigo,ano,dre)
    LPA =adicionaG(LPA)
    print(LPA)
    mediaLPA = LPA['VL_CONTA'].mean()
    mediaG = LPA['G'].mean()
    printMedias(LPA)
    valorIntrinseco = getValorIntrinseco(mediaG, mediaLPA)
    print(valorIntrinseco)


#retorna o lucro por acao da empresa, se baseando no DRE
def getLucrosEmpresa(codCVM,dre):
    empresa = dre[dre['CD_CVM'] == codCVM]
    #contas = empresa[['CD_CONTA', 'DS_CONTA'].drop_duplicates().set_index('CD_CONTA')

    # Seleciona qual conta realizar calculo
    #ALTERAR somar os valores e nao só verificar 1, entender o que é esse código
    LPA = empresa[empresa['CD_CONTA'] == '3.99.01.01']
    if (LPA.size > 0):
        print("Maior11")
    else:
        LPA = empresa[empresa['CD_CONTA'] == '3.99.01.02']
        if (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
            print("Maior12")
        else:
            LPA = empresa[empresa['CD_CONTA'] == '3.99.02.01']
            if (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
                print("Maior21")
            else:
                LPA = empresa[empresa['CD_CONTA'] == '3.99.02.02']
                if (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
                    print("Maior22")
                else:
                    LPA = empresa[empresa['CD_CONTA'] == '3.99']
                    print(LPA)
                    if (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
                        print("Maior22")
                    else:
                        LPA = empresa[empresa['CD_CONTA'] == '3.99.01']
                        if (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
                            print("Maior9901")
                        else:
                            LPA = empresa[empresa['CD_CONTA'] == '3.99.02']
                            print("Maior9902")
                            if not (LPA.size > 0 and LPA['VL_CONTA'].mean() != 0):
                                print("Erro")
                                raise Exception("No VPL")
    print(LPA);
    print(LPA["VL_CONTA"]);
    return LPA;



print(getCodigoCVM('IRBR3.SA'))
dre = pd.read_csv('DADOS//itr_dfp_cia_aberta__cia_aberta_DRE_ind_2011_2022.csv')
#dre = dre[dre['ORDEM_EXERC'] == "ÚLTIMO"]



#printContas('24180',2016,dre)
#getValorPresenteTopEmpresasIbov(85, 2016,dre)
#getValorPresenteTopEmpresasIbov(50, 2016,dre)

getLucrosEmpresa(22187,dre);


#proximo
    
    

#calcular Payout= 1-(G/ROE) tem outros calculos possiveis
#calcular DPA estimativa proximos 5 anos LPA*Payout

#calcular Ke (custo de oportunidade) ou assumir valor 15% ou=RiskFree+(beta*premio Mercado)
#calcular perpetuidade= DPA_ultimo *(1+G)/(Ke-G)

#Calculo do valor presente dos DPA em relação ao Ke
#Calculo valor presente Perpetuidade = Perpetuidade/(1+Ke)^5

#Valor Intrinseco = valor presente dos DPA + valor presente Perpetuidade





#validar caso algum valor seja muito fora da média, apontar para reavaliação,
# para correção de valor errado, ou analise manual do ativo, exemplo EQTL3.SA e PRIO3.SA
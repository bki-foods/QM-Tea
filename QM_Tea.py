#!/usr/bin/env python3

import datetime
import urllib
import pandas as pd
from sqlalchemy import create_engine
import pyodbc


# Define server connection and SQL query:
server = r'sqlsrv04\tx'
db = 'TXprodDWH'
con = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db)
query = """ SELECT V.[Varenr] AS [ItemNo], V.[Udmeldelsesstatus] AS [Status]
        ,V.[Nettovægt] * ISNULL(SVP.[Qty],0) AS [Quantity], SVP.[Amount]
        ,SVP.[Cost], V.[Dage siden oprettelse] AS [Days]
		,CASE WHEN V.[Udmeldelsesstatus] = 'Er udgået'
			THEN 0 ELSE ISNULL(SVP.[Count],0) END AS [Count]
		,V.[Vareansvar] AS [Department]
        FROM [TXprodDWH].[dbo].[Vare_V] AS V
        LEFT JOIN (
        SELECT [Varenr], -1 * SUM(ISNULL([Faktureret antal],0)) AS [Qty]
        ,SUM([Oms excl. kampagneAnnonce]) AS [Amount]
        ,SUM([Kostbeløb]) AS [Cost], COUNT(*) AS [Count]
        FROM [TXprodDWH].[dbo].[factSTATISTIK VAREPOST_V]
        WHERE [VarePosttype] IN (-1, 1)
            AND [Bogføringsdato] >= DATEADD(year, -1, getdate())
        GROUP BY [Varenr]
        ) AS SVP
        ON V.[Varenr] = SVP.[Varenr]
        WHERE V.[Varekategorikode] = 'TE'
            AND V.[Varenr] NOT LIKE '9%'
            AND V.[Salgsvare] = 'Ja'
			AND (V.[Produktionskode] IN ('PAK PL TE', 'PAKKET TE') 
				OR V.[Underproduktgruppekode] IN ('815','820','910','912'))
			AND V.[Underproduktgruppekode] NOT IN ('940','942') """

# Read query and create Profit calculation:
df = pd.read_sql(query, con)
df['MonetaryValue'] = df['Amount'] - df['Cost']

# Empty dataframe for quantiles and consolidation:
dfQuan = pd.DataFrame()
dfCons = pd.DataFrame()

# Quantity and MonetaryValue score - bigger numbers are better:
def qm_score(x, para, dic):
    if x <= dic[para][0.25]:
        return 4
    elif x <= dic[para][0.5]:
        return 3
    elif x <= dic[para][0.75]:
        return 2
    else:
        return 1


# Create timestamp and other variables
now = datetime.datetime.now()
scriptName = 'TEST_QM_Tea.py'
executionId = int(now.timestamp())
tType = 'Te, egenproduceret'
departments = df.Department.unique()

# =============================================================================
#                        SKUs with sales
# =============================================================================
dfSales = df.loc[df['Count'] != 0]

for dep in departments:
# Create dataframe and skip if it's empty
    dfSalesTea = dfSales.loc[dfSales['Department'] == dep]
    if len(dfSalesTea) != 0:
# Define quantiles for:
        quantiles = dfSalesTea.quantile(q=[0.25, 0.5, 0.75]).to_dict()
# Identify quartiles per measure for each product:
        dfSalesTea.loc[:, 'QuantityQuartile'] = dfSales['Quantity'].apply(qm_score, args=('Quantity', quantiles,))
        dfSalesTea.loc[:, 'MonetaryQuartile'] = dfSales['MonetaryValue'].apply(qm_score, args=('MonetaryValue', quantiles,))
# Concetenate Quartile measurements to single string:
        dfSalesTea.loc[:, 'Score'] = dfSalesTea.QuantityQuartile * 10 + dfSalesTea.MonetaryQuartile
# Create data stamps for dataframe and append to consolidated dataframe:
        dfSalesTea.loc[:, 'Timestamp'] = now
        dfSalesTea.loc[:, 'Type'] = dep + '/' + tType
        dfSalesTea.loc[:, 'ExecutionId'] = executionId
        dfSalesTea.loc[:, 'Script'] = scriptName
        dfCons = pd.concat([dfCons, dfSalesTea])
# Create quantile dataframe
        dfQuan = pd.DataFrame.from_dict(quantiles)
        dfQuan.loc[:, 'Type'] = tType
        dfQuan.loc[:, 'Timestamp'] = now
        dfQuan.loc[:, 'ExecutionId'] = executionId
        dfQuan.loc[:, 'Quantile'] = dfQuan.index

# =============================================================================
#                        SKUs without sales
# =============================================================================
dfNoSales = df.loc[df['Count'] == 0]

dfNoSales.loc[:, 'Timestamp'] = now
dfNoSales.loc[:, 'Score'] = dfNoSales['Days'].apply(lambda x: 1 if x > 90 else 2)
dfNoSales.loc[dfNoSales['Status'] == 'Er udgået', 'Score'] = 0
dfNoSales.loc[:, 'Type'] = dfNoSales['Department'] + '/' + tType
dfNoSales.loc[:, 'ExecutionId'] = executionId
dfNoSales.loc[:, 'Script'] = scriptName

# =============================================================================
#                       Prepare dataframes for SQL insert
# =============================================================================
ColsSales = ['ExecutionId', 'Timestamp', 'ItemNo', 'Quantity', 'MonetaryValue', 'Score', 'Type', 'Script']
ColsNoSales = ['ExecutionId', 'Timestamp', 'ItemNo', 'Score', 'Type', 'Script']
ColsQuan = (['ExecutionId', 'Timestamp', 'Type', 'Quantile', 'Quantity',
             'MonetaryValue'])

dfCons = dfCons[ColsSales]
dfNoSales = dfNoSales[ColsNoSales]
dfQuan = dfQuan[ColsQuan]

# =============================================================================
#                               Dataframe for logging
# =============================================================================
dfLog = pd.DataFrame(data= {'Date':now, 'Event':scriptName}, index=[0])
# =============================================================================
#                               Insert SQL
# =============================================================================
params = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 10.0};SERVER=sqlsrv04;DATABASE=BKI_Datastore;Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
dfCons.to_sql('ItemSegmentation', con=engine, schema='dev', if_exists='append', index=False)
dfNoSales.to_sql('ItemSegmentation', con=engine, schema='dev', if_exists='append', index=False)
dfQuan.to_sql('ItemSegmentationQuantiles', con=engine, schema='dev', if_exists='append', index=False)
dfLog.to_sql('Log', con=engine, schema='dbo', if_exists='append', index=False)

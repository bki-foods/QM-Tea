#!/usr/bin/env python3

import datetime
import urllib
import pandas as pd
from sqlalchemy import create_engine


# Define server connection and SQL query:
server_tx = r"sqlsrv04\tx"
db_tx_prod_dwh = "TXprodDWH"
params_tx_prod_dwh = f"DRIVER={{SQL Server Native Client 11.0}};SERVER={server_tx};DATABASE={db_tx_prod_dwh};trusted_connection=yes"
con_tx_prod_dwh = create_engine('mssql+pyodbc:///?odbc_connect=%s' % urllib.parse.quote_plus(params_tx_prod_dwh))

server_04 = "sqlsrv04"
db_ds = "BKI_Datastore"
params_ds = f"DRIVER={{SQL Server Native Client 11.0}};SERVER={server_04};DATABASE={db_ds};trusted_connection=yes"
con_ds = create_engine('mssql+pyodbc:///?odbc_connect=%s' % urllib.parse.quote_plus(params_ds))

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
        WHERE [VarePosttype] = 1
            AND [Bogføringsdato] >= DATEADD(year, -1, getdate())
        GROUP BY [Varenr]
        ) AS SVP
        ON V.[Varenr] = SVP.[Varenr]
        WHERE V.[Varekategorikode] = 'TE'
            AND V.[Varenr] NOT LIKE '9%'
            AND V.[Salgsvare] = 'Ja'
			AND (V.[Produktionskode] IN ('PAK PL TE', 'PAKKET TE') 
				OR V.[Underproduktgruppekode] IN ('815','820','910','912'))
			AND V.[Underproduktgruppekode] NOT IN ('940','942')
			AND V.[DW_Account] = 'BKI foods a/s' """

# Read query and create Profit calculation:
df = pd.read_sql(query, con_tx_prod_dwh)
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
scriptName = 'QM_Tea.py'
executionId = int(now.timestamp())
tType = 'TE, EGENPRODUKTION'
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
# Append quantiles to dataframe
        dfTemp = pd.DataFrame.from_dict(quantiles)
        dfTemp.loc[:, 'Type'] = dep + '/' + tType
        dfTemp.loc[:, 'Quantile'] = dfTemp.index
        dfQuan = pd.concat([dfTemp, dfQuan], sort=False)
        dfQuan.loc[:, 'Timestamp'] = now
        dfQuan.loc[:, 'ExecutionId'] = executionId

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
dfLog = pd.DataFrame(data= {'Date':now, 'Event':scriptName, 'Note':'Execution id: ' + str(executionId)}, index=[0])
# =============================================================================
#                               Insert SQL
# =============================================================================
dfCons.to_sql('ItemSegmentation', con=con_ds, schema='seg', if_exists='append', index=False)
dfNoSales.to_sql('ItemSegmentation', con=con_ds, schema='seg', if_exists='append', index=False)
dfQuan.to_sql('ItemSegmentationQuantiles', con=con_ds, schema='seg', if_exists='append', index=False)
dfLog.to_sql('Log', con=con_ds, schema='dbo', if_exists='append', index=False)

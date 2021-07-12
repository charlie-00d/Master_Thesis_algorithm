
import numpy as np
import pandas as pd
import requests
import io
import datetime as datetime

#SX7P data high_low_close_open_prices from https://www.wsj.com/market-data/quotes/index/XX/XSTX/FX7/historical-prices
df = pd.read_csv('HistoricalPrices.csv')
df['Date'] = pd.to_datetime(df['Date'])
#remove non-trading days historical data
#https://www.xetra.com/resource/blob/281576/967747fbc75fa120c2142e05d2e40711/data/trading-calendar
non_trading_days = ['01-01-2014','21-04-2014','01-05-2014','03-10-2014','24-12-2014','25-12-2014','26-12-2014','31-12-2014',
                    '01-01-2015','06-04-2015','25-05-2015','24-12-2015','25-12-2015','31-12-2015',                  
                    '01-01-2016','28-03-2016','16-05-2016','03-10-2016','26-12-2016',    
                    '14-04-2017','17-04-2017','01-05-2017','05-06-2017','03-10-2017','31-10-2017','25-12-2017','26-12-2017', 
                    '01-01-2018','30-03-2018','02-04-2018','01-05-2018','21-05-2018','03-10-2018','24-12-2018','25-12-2018','26-12-2018','31-12-2018',
                    '01-01-2019','19-04-2019','22-04-2019','01-05-2019','10-06-2019','03-10-2019','24-12-2019','25-12-2019','26-12-2019','31-12-2019',
                    '01-01-2020','10-03-2020','13-03-2020','01-05-2020','01-06-2020','24-12-2020','25-12-2020','31-12-2020','01-01-2020',     
                ]
non_trading_days = (datetime.datetime.strptime(a,'%d-%m-%Y') for a in non_trading_days)

SX7P = df[~df['Date'].isin(non_trading_days)]

########## NEW
entrypoint = 'https://sdw-wsrest.ecb.europa.eu/service/'
resource = 'data'
parameters = {'startPeriod' : '2014-01-01',
        'endPeriod' : '2020-12-30'}

data = {'currency_pair': ['daily', 'EXR' + '/' + 'D.USD.EUR.SP00.A'], 
        'debt_sec': ['monthly', 'BSI' + '/' + 'M.U2.N.R.L40.H.1.A1.0000.Z01.E'],
        'exc_reserves': ['monthly', 'BSI' + '/' + 'M.U2.N.R.LRE.X.1.A1.3000.Z01.E'],
        'bank_notes_in_circulation':['monthly', 'ILM' + '/' + 'M.U2.C.L010000.Z5.EUR'],
        'current_accounts': ['monthly', 'ILM' + '/' + 'M.U2.C.L020100.U2.EUR'],
        'interest_rate_on_loans': ['monthly', 'MIR' + '/' + 'M.U2.B.A20.A.I.A.2240.EUR.O'],
        'interest_rate_on_deposits': ['monthly', 'MIR' + '/' + 'M.U2.B.L21.A.I.A.2230.EUR.N'],
        'index_debt_securities': ['monthly', 'SEC' + '/' + 'M.A1.1000.F33000.N.I.EUR.E.Z'],
        'HICP': ['monthly', 'ICP' + '/' + 'M.U2.N.000000.4.INX'],
        'building_permits': ['monthly', 'STS' + '/' + 'M.I8.N.BPER.CC1109.4.000'],
        'industrial_new_orders': ['monthly', 'STS' + '/' + 'M.I8.Y.ORDT.NSC002.3.000' ],
        'commodities_index': ['monthly', 'STS' + '/' + 'M.I8.N.ECPE.CFOOD0.3.000'],
        'total_employment': ['daily', 'ENA' + '/' + 'Q.N.I8.W2.S1.S1._Z.EMP._Z._T._Z.PS._Z.N'],
        'labour_cost_index': ['daily', 'LCI' + '/' + 'Q.I8.W.LCI_T.BTS'],
        'import_price_index': ['daily', 'STS' + '/' + 'M.I8.N.IMPX.2C0000.4.000'],
        'total_trade_volume': ['daily', 'TRD' + '/' + 'M.I8.N.M.TTT.J8.4.VAL'],
        'interest_rate_10yrs': ['daily', 'IRS' + '/' + 'M.D0.L.L40.CI.0000.Z01.N.Z'],
        'yield_curve_spot_rate': ['daily', 'YC' + '/' + 'B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y'],
        'revenue_EU19': ['daily', 'GFS' + '/' + 'A.N.I8.W0.SZX.S1.N.C.OTE._Z._Z._Z.XDC_R_B1GQ._Z.S.V.N._T'],
        'expenses_EU19': ['daily', 'GFS' + '/' + 'A.N.I8.W0.SZX.S1.N.D.OTR._Z._Z._Z.XDC_R_B1GQ._Z.S.V.N._T'],
        'gov_change_debt_%_GDP_EU19': ['daily', 'GFS' + '/' + 'Q.N.I8.W0.S13.S1.C.L.LX.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.CY._T']
}

for attr in data.keys():

        # query
        response = requests.get(entrypoint + resource + '/'+data[attr][1], params = parameters, headers = {'Accept': 'text/csv'})
        df = pd.read_csv(io.StringIO(response.text))

        # select columns
        df = df.loc[:,['TIME_PERIOD','OBS_VALUE']]
        df.rename(columns={'OBS_VALUE': attr}, inplace=True)
        df['TIME_PERIOD'] = pd.to_datetime(df['TIME_PERIOD'])

        # merge in terms of whether data is provided on a daily or monthly basis
        if data[attr][0]=='daily':
                SX7P = pd.merge(SX7P, df, left_on=SX7P['Date'],
                        right_on=df['TIME_PERIOD'],
                        how='left')
        else:
                SX7P = pd.merge(SX7P, df, left_on=SX7P['Date'].apply(lambda x: (x.year, x.month)),
                        right_on=df['TIME_PERIOD'].apply(lambda y: (y.year, y.month)),
                        how='left')

        # drop extra attributes
        SX7P.drop(['key_0','TIME_PERIOD'], axis = 1 , inplace = True)
x_18 = SX7P.loc[(SX7P['Date']>= '01-12-2017') & (SX7P['Date']<='28-12-2018')]

#daily trading volume and %variance
#algorithm_datset_b = pd.read_csv('algorithm_dataset_a.csv')
volume = pd.read_csv('Stoxx.csv')
volume.dtypes
SX7P['Volume_traded'] = volume['Vol.']
SX7P['%_variance'] = volume['% var.']
SX7P.drop(labels=(0,1), axis = 1,)
SX7P.columns
SX7P.drop(['Unnamed: 0','bank_loan_margin_competition'], axis = 1, inplace=True)


#macroeconomic indicators #https://stats.oecd.org/#
M1 = pd.read_csv('M1.csv') #M1
EUR_M1 = M1.loc[M1['LOCATION'] == 'EA19',['TIME','Value']]
EUR_M1[~EUR_M1['TIME'].str.contains('Q')]
EUR_M1.drop(EUR_M1.index[np.where(EUR_M1.index > 399)[0]], inplace = True)
EUR_M1['TIME'] = pd.to_datetime(EUR_M1['TIME'])
SX7P = pd.merge(SX7P,EUR_M1, left_on=SX7P['Date'].apply(lambda x: (x.year, x.month)),
         right_on=EUR_M1['TIME'].apply(lambda y: (y.year, y.month)),
         how='left')

financial_indicators = pd.read_csv('financial_indicators.csv') #real effective exchange rate
financial_indicators['TIME'] = pd.to_datetime(financial_indicators['TIME'])
financial = financial_indicators.loc[:,['TIME','Value']]
SX7P = pd.merge(SX7P,financial, left_on=SX7P['Date'].apply(lambda x: (x.year, x.month)),
         right_on=financial['TIME'].apply(lambda y: (y.year, y.month)),
         how='left')


prices_indexes = pd.read_csv('prices_indexes.csv') #CPI
CPI = prices_indexes.loc[:,['TIME','Value']]
CPI.drop(CPI.index[np.where(CPI.index > 399)[0]], inplace = True)
CPI['TIME'] = pd.to_datetime(CPI['TIME'])
SX7P = pd.merge(SX7P ,CPI, left_on=SX7P['Date'].apply(lambda x: (x.year, x.month)),
         right_on=CPI['TIME'].apply(lambda y: (y.year, y.month)),
         how='left')


confidence_indicators = pd.read_csv('confidence_indicators.csv') #confidence sentiment
confidence_indicators.drop(axis = 0, index = 121, inplace=True)
confidence = confidence_indicators.loc[:,['TIME','Value']]
confidence['TIME'] = pd.to_datetime(confidence['TIME'])
SX7P = pd.merge(SX7P,confidence, left_on=SX7P['Date'].apply(lambda x: (x.year, x.month)),
         right_on=confidence['TIME'].apply(lambda y: (y.year, y.month)))


SX7P.to_csv('algorithm_dataset.csv')

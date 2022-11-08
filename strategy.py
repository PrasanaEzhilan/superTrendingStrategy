#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import datetime
from datetime import datetime
import warnings
import mysql.connector
warnings.filterwarnings('ignore')


# In[2]:


import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="stockprice"
)
mycursor = mydb.cursor()


# In[3]:


def Get_LogReturn(df, var_in, var_out):
    df[var_out] = np.log(df[var_in].astype('float64')).diff()
    return df


# In[4]:


mycursor.execute("SELECT * FROM stockprice.price")
temp=mycursor.fetchall()


# In[5]:


df=pd.DataFrame(temp, columns=['Symbols','Price','Date'])


# In[6]:


df['Date'] = pd.to_datetime(df['Date'], utc=True)
df=df.set_index("Date")


# In[7]:


momentum=pd.DataFrame()
volatality=pd.DataFrame()
price=pd.DataFrame()
for ticker in df.Symbols.unique():
   temp=df[df['Symbols']==ticker]
   price.loc[ticker,"price"]=temp.iloc[-1,-1]
   temp=Get_LogReturn(temp,'Price','return')
   temp['Volatality']=temp['return'].rolling(63).std()
   month1 = pd.Series(temp.index.month)
   month2 = pd.Series(temp.index.month).shift(-1)
   mask = (month1 != month2)
   df_monthend=temp[mask.values]
   df_monthend['momentum']=(((df_monthend['Price']/df_monthend['Price'].shift(1))-1)+((df_monthend['Price']/df_monthend['Price'].shift(3))-1)+((df_monthend['Price']/df_monthend['Price'].shift(6))-1)+((df_monthend['Price']/df_monthend['Price'].shift(12))-1))/4
   momentum[ticker]=df_monthend['momentum']
   volatality[ticker]=df_monthend['Volatality']
   print(ticker)


# In[8]:


breakmomentum=momentum.iloc[-1,:].quantile(0.98)
x=volatality.iloc[-1,:]*(momentum.iloc[-1,:]>breakmomentum)
x=1/x
x=x.replace([np.inf, -np.inf], np.nan)
x=x/x.sum()
final=pd.DataFrame()
final['weight']=x


# In[9]:


final["price"]=price["price"]
final["temp"]=final["price"]/final["weight"]
Maxtemp=final["temp"].max()
Maxweight=float(final.loc[final['temp']==Maxtemp,"weight"])
Maxprice=float(final.loc[final['temp']==Maxtemp,"price"])
final["Stock_Count"]=(Maxprice/final["price"])*(final["weight"]/Maxweight)
final=final.dropna()
final["Stock_Count"]=final["Stock_Count"].astype(int)
final.drop(['temp'], axis=1, inplace=True)
final["portfolio_Value"]=final["Stock_Count"]*final["price"]
print("Total investment in multiples of ",final["portfolio_Value"].sum())


# In[10]:


final


# In[ ]:





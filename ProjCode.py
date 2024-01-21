import pandas as pd
import sqlalchemy as sa
from datetime import datetime
import calendar
import numpy as np
import seaborn as sns

#reading data
df=pd.read_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\Final.csv")
cd=pd.read_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\CustF.txt",delimiter=",",encoding='latin1')
print(df)
print(cd)
print(cd.columns)
print("Data type in df:", df['Customer_ID'].dtype)
print("Data type in cd:", cd['Customer_ID'].dtype)
#they turn out to be of same data type so no need to change
md=pd.merge(df, cd, on='Customer_ID', how='right')
print(md)
#extracting the merged data
md.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\mergedV1.csv")
#concatenating the first name and last name columns into one single column
md['CustFN'] = md[' Customer_FirstName'] + ' ' + md[' Customer_LastName']
md=md.drop([' Customer_FirstName', ' Customer_LastName'], axis=1)
print(md)
#extracting the transformed data
md.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\transV1.csv")
NaN= md.isna().sum()
print(NaN)
md['Birth_Date'] = pd.to_datetime(md['Birth_Date'], format='%m/%d/%Y', errors='coerce')
md['Date'] =pd.to_datetime(md['Date'], format='%m/%d/%Y', errors='coerce')
age_in_years = (md['Date'] - md['Birth_Date']) / pd.Timedelta(days=365.25)
md['age_when_order'] = age_in_years.astype(float)
md.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\transV2.csv")
md['Date']=pd.to_datetime(md['Date'])
md['month']=md['Date'].dt.month
md.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\transv3.csv", index=False)
#changing month numbers to month names for better understanding
md['month'] = md['month'].apply(lambda x: calendar.month_name[x])
md.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\transV4.csv", index=False)
#changing unit price format
md['Unit price'] = pd.to_numeric(md['Unit price'], errors='coerce')
md['Unit price'] = md['Unit price'].astype('float')
#calculating the total income per order
md['total_income'] = md['Unit price'] * md['Quantity']
md.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\transV5.csv", index=False)
#changing unit cost format
md['Unit cost'] = pd.to_numeric(md['Unit cost'], errors='coerce')
md['Unit cost'] = md['Unit cost'].astype('float')
#calculating total cost per order
md['total_cost'] = md['Unit cost'] * md['Quantity']
md.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\transV6.csv", index=False)
#calculating the total profit per order
md['total_profit'] = md['total_income'] - md['total_cost']
md.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\transv7.csv", index=False)
#changing shipping cost format
md['shipping cost'] = pd.to_numeric(md['shipping cost'], errors='coerce')
md['shipping cost'] = md['shipping cost'].astype('float')
print(md.dtypes)
#in case of missing values in a numeric column
numerical_columns = md.select_dtypes(include=['int', 'float']).columns
md[numerical_columns] = md[numerical_columns].fillna(md[numerical_columns].mean())
print(md)
#checking for missing values
print(md.isnull().sum())
#dropping the null values in product line and customer adress
md = md.dropna()
print(md.isnull().sum())
md.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\FinalTrans.csv", index=False)
#extracting the dimensions
Time_dim = md[['Date', 'Time', 'month']].copy()
Time_dim.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\F_D\\TimeDim.csv", index=False)
Cust_dim=md[['Customer_ID', 'Gender', 'age_when_order','CustFN','Country','Customer type']].copy()
Cust_dim.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\F_D\\CustDim.csv", index=False)
Prod_dim=md[['Product line','Unit price','Unit cost','Rating']]
Prod_dim.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\F_D\\ProdDim.csv", index=False)
Order_dim=md[['Invoice ID','Product line','Date','Quantity','Shipment_mode','shipping cost','Payment','warehouse_name']].copy()
Order_dim.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\F_D\\OrderDim.csv", index=False)
Fact_tab=md[['Customer_ID','Invoice ID','month','total_income','total_cost','total_profit']]
Fact_tab.to_csv("C:\\Users\\malek\\OneDrive\\Bureau\\Bi2\\F_D\\FactTab.csv", index=False)


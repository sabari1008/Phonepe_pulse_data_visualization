#colab URL " https://colab.research.google.com/drive/1y2ZHgbZaLkMoANowvNFcE_LrreKkkcSY#scrollTo=Mi-HwQuoUNSJ "  to extract data from github and download as a csv file.


import mysql.connector as sql
import pandas as pd

mydb = sql.connect(host="localhost",
                   user="root",
                   password="SQL123@#sql",
                   database= "phonepe_data"
                  )
mycursor = mydb.cursor(buffered=True)


df_agg_trans = pd.read_csv(r'C:\Users\subash\df_phonepe/agg_trans.csv')
df_agg_user = pd.read_csv(r'C:\Users\subash\df_phonepe/agg_user.csv')
df_map_trans = pd.read_csv(r'C:\Users\subash\df_phonepe/map_trans.csv')
df_map_user = pd.read_csv(r'C:\Users\subash\df_phonepe/map_user.csv')
df_top_trans = pd.read_csv(r'C:\Users\subash\df_phonepe/top_trans.csv')
df_top_user = pd.read_csv(r'C:\Users\subash\df_phonepe/top_user.csv')


mycursor.execute("create table agg_trans (State varchar(100), Year int, Quarter int, Transaction_type varchar(100), Transaction_count int, Transaction_amount double)")

for i,row in df_agg_trans.iterrows():
    #here %S means string values 
    sql = "INSERT INTO agg_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    # the connection is not auto committed by default, so we must commit to save our changes
    mydb.commit()

mycursor.execute("create table agg_user (State varchar(100), Year int, Quarter int, Brands varchar(100), Count int, Percentage double)")

for i,row in df_agg_user.iterrows():
    sql = "INSERT INTO agg_user VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table map_trans (State varchar(100), Year int, Quarter int, District varchar(100), Count int, Amount double)")


for i,row in df_map_trans.iterrows():
    sql = "INSERT INTO map_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table map_user (State varchar(100), Year int, Quarter int, District varchar(100), Registered_user int, App_opens int)")


for i,row in df_map_user.iterrows():
    sql = "INSERT INTO map_user VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table top_trans (State varchar(100), Year int, Quarter int, Pincode int, Transaction_count int, Transaction_amount double)")

for i,row in df_top_trans.iterrows():
    sql = "INSERT INTO top_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table top_user (State varchar(100), Year int, Quarter int, Pincode int, Registered_users int)")

for i,row in df_top_user.iterrows():
    sql = "INSERT INTO top_user VALUES (%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.close()
mydb.close()

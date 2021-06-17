from datetime import date
import re
import time
import requests
from threading import Thread, ThreadError
import threading
from requests.api import get
from urllib3.exceptions import InsecureRequestWarning #added
import urllib3
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning) #added
from bs4 import BeautifulSoup
import pandas as pd
import re
import pypyodbc

DATABASE_CONFIG ={
    'Driver': 'SQL Server',
    'Server': 'LAPTOP-N94DSRJ8\VINHSEVER',
    'Database': 'Football',
    'UID': 'sa',
    'Password': '123'
}
def getConnection():
    try:
        connection = pypyodbc.connect("Driver= {"+DATABASE_CONFIG["Driver"]+"} ;Server=" + DATABASE_CONFIG["Server"] + ";Database=" + DATABASE_CONFIG["Database"] + ";uid=" + DATABASE_CONFIG["UID"] + ";pwd=" + DATABASE_CONFIG["Password"])
    except:
        print("Connection Error!")
        return
    return connection


def CUD_SQL(input_query,param_list):
    connection= getConnection()
    try:
        query=input_query
        cursor= connection.cursor()
        cursor.execute(query,param_list)
        print("CUD success!!")
        connection.commit()
    except Exception as e:
        print("CUD went wrong!!")
        print(e)
        connection.rollback()
    finally:
        connection.close()

def R_SQL(input_query,param_list):
    connection= getConnection()
    cursor=connection.cursor()
    try:
        cursor.execute(input_query,param_list)
        print('Query success')
        return cursor
    except Exception as e:
        print("R went wrong!!")
        return
    finally:
        print("OK")
        connection.close()

# param_list=[]
# param_list.append('Джахонгир Абдумуминов_185_2001-02-19')
# param_list.append('Liem Dieu Tran')
# param_list.append('Джахонгир Абдумуминов')
# param_list.append('https://img.a.transfermarkt.technology/portrait/header/default.jpg?lm=1')
# param_list.append('2001-02-19')
# param_list.append('158')
# param_list.append('r')
# param_list.append('r')
# CUD_SQL("insert into Player values(?,?,?,?,?,?,?,?)",param_list)

def CUD_CLB(clb_list_df):
    query="insert into CLB values "
    try:
        param_list= []
        for i in clb_list_df.index:
            param_list.append(clb_list_df['name'][i])
            param_list.append(clb_list_df['img_url'][i])
            query+="(?,?)"
            if i == len(clb_list_df)-1:
                query+=";"
            else:
                query+=','
        CUD_SQL(query,param_list)
    except Exception as e:
        print("Insert CLB error!!")
        print(e)

def CUD_Player(player_list_df):
    connection=getConnection()
    try:
        for i in player_list_df.index:
            query="insert into Player values (?,?,?,?,?,?,?,?)"
            # query+="(N'"+str(player_list_df['key'][i])+"',N'"+str(player_list_df['name'][i])+"',N'"+str(player_list_df['Name in home country'][i])+"','"+str(player_list_df['img_url'][i])+"','"+player_list_df['Date of birth'][i]+"',"+str(player_list_df['Height'][i])+",'"+str(player_list_df['Foot'][i])+"','"+str(player_list_df['Citizenship'][i])+"')"
            cursor=connection.cursor()
            cursor.execute(query,[str(player_list_df['key'][i]),str(player_list_df['name'][i]),str(player_list_df['Name in home country'][i]),str(player_list_df['img_url'][i]),player_list_df['Date of birth'][i],str(player_list_df['Height'][i]),str(player_list_df['Foot'][i]),str(player_list_df['Citizenship'][i]),])
        print("Insert success")
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(query)
        print("Insert Error")
        print(e)

def CUD_Player_Properties(player_list_df):
    connection=getConnection()
    try:
        for i in player_list_df.index:
            query="insert into Player_Properties values (?,?,?,?)"
            cursor=connection.cursor()
            cursor.execute(query,[str(player_list_df['key'][i]),str('2021-05-23'),str('position'),str(player_list_df['Position'][i]),])
            # cursor.execute(query,[str(player_list_df['key'][i]),str('2021-05-23'),str('value'),str(0)])
            # cursor.execute(query,[str(player_list_df['key'][i]),str('2021-05-23'),str('performance'),str(0)])
            # cursor.execute(query,[str(player_list_df['key'][i]),str('2021-05-23'),str('number'),str(player_list_df['number'][i])])
        print("Insert success")
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(query)
        print("Insert Error")
        print(e)

def CUD_Player_Club(player_list_df):
    connection=getConnection()
    try:
        for i in player_list_df.index:
            query="insert into Player_Clubs values (?,?,?,?,?)"
            cursor=connection.cursor()
            cursor.execute(query,[str(player_list_df['key'][i]),str(player_list_df['club'][i]),'2021-05-23',None,None])
        print("Insert success")
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(query)
        print("Insert Error")
        print(e)



headers = {"User-Agent":"Mozilla/5.0"}
my_request = requests.get('https://www.transfermarkt.com/v-league-1/startseite/wettbewerb/VIE1', verify=False,headers=headers)
my_soup = BeautifulSoup(my_request.text, "lxml")

clb_list=[]
player_list=[]

def transform_country_name(str):
    if str=="nan":
        str="noinfo"
    return str

def transform_height(str):
    if str=="nan":
        str="0"
    str=str.replace(",","")
    str=str.replace("m","")
    char=u'\xa0'
    str=str.replace(char,"")
    return str

def transform_foot(str):
    if str=="nan" or str=="" or str==None:
        str='noinfo'
    return str

def is_all_thread_done():
    while(1):
        if threading.active_count() ==1:
            print("Everything done")
            print(clb_list)
            break
        print("Have "+str(threading.active_count())+" thread working============================")
        time.sleep(1)

def Get_Data_CLB(clb_el):
    try:
        clb_el_url= clb_el.select_one('a')['href'] 
        try:
            clb_el_request= requests.get("https://www.transfermarkt.com"+clb_el_url+"/plus/1", verify=False,headers=headers).text
        except:
            print("CLB request connection error")
            return
        clb_el_soup= BeautifulSoup(clb_el_request,"lxml")

        # Get Clb_name and Clb_img_url
        clb_img_url= clb_el_soup.select_one('.dataBild  img')['src']
        clb_name= clb_el_soup.select_one('.dataHeader h1 span').text
        clb_dic={}
        clb_dic['img_url']= clb_img_url
        clb_dic['name']= clb_name
        clb_list.append(clb_dic)

        player_el_list= clb_el_soup.select('#yw1 tbody >tr')
        for player_el in player_el_list:
            player_el_url= player_el.select_one('.hauptlink a')['href']
            try:
                player_el_request= requests.get("https://www.transfermarkt.com"+player_el_url, verify=False,headers=headers).text
            except:
                print("Player request connection error")
                return
            player_el_soup= BeautifulSoup(player_el_request,"lxml")
            
            player_dic={}
            player_img_url= player_el_soup.select_one('.dataBild img')['src']
            player_name= player_el_soup.select_one('.dataName h1').text
            if player_el_soup.select_one('.dataName span') == None:
                player_number= None
            else:
                player_number= player_el_soup.select_one('.dataName span').text.replace("#","")

            player_value= 0
            player_performance= 3
            player_dic['value']=player_value
            player_dic['performance']=player_performance
            player_dic['club']= clb_name

            detail_info_el_tr_list= player_el_soup.select('table.auflistung tr')

            for el in detail_info_el_tr_list:
                key= el.select_one('th').text.strip().replace(":","")
                value= el.select_one('td').text.strip()
                player_dic[key]=value

            player_dic['name']=player_name
            player_dic['img_url']=player_img_url
            player_dic['number']=player_number
            print(player_dic)
            player_list.append(player_dic)
    except Exception as e:
        print("Error "+e)
        return

class CLB_Thread(Thread):
    def __init__(self,clb_el):
        super(CLB_Thread,self).__init__()
        self.clb_el= clb_el
    def run(self):  
        Get_Data_CLB(self.clb_el)

# Get all clb el
clb_list_el= my_soup.select('#yw1 tbody tr')
for clb_el in clb_list_el:
    try:
        el_thread= CLB_Thread(clb_el)
        el_thread.start()
    except:
        print("Thread Error")

is_all_thread_done()
player_list_df=pd.DataFrame(player_list)

if "Age" in player_list_df:
    player_list_df.pop("Age")
if "Place of birth" in player_list_df:
    player_list_df.pop("Place of birth")
if "Current club" in player_list_df:
    player_list_df.pop("Current club")
if "Joined" in player_list_df:
    player_list_df.pop("Joined")
if "Contract expires" in player_list_df:
    player_list_df.pop("Contract expires")
if "On loan from" in player_list_df:
    player_list_df.pop("On loan from")
if "Contract there expires" in player_list_df:
    player_list_df.pop("Contract there expires")
if "Date of last contract extension" in player_list_df:
    player_list_df.pop("Date of last contract extension")
if "Player agent" in player_list_df:
    player_list_df.pop("Player agent")
if "Social-Media" in player_list_df:
    player_list_df.pop("Social-Media")
if "Full name" in player_list_df:
    player_list_df.pop("Full name")
if "Outfitter" in player_list_df:
    player_list_df.pop("Outfitter")

player_list_df['Name in home country']=player_list_df['Name in home country'].apply(lambda x:transform_country_name(str(x)))
player_list_df['Height'] = player_list_df['Height'].apply(lambda x: transform_height(str(x)))
player_list_df['Date of birth']= player_list_df['Date of birth'].apply(lambda x: x.replace(" Happy Birthday",""))
player_list_df['Date of birth']= player_list_df['Date of birth'].apply(lambda x: x.replace(",","").replace(" ","/"))
player_list_df['Date of birth']= pd.to_datetime(player_list_df['Date of birth'])
player_list_df['Date of birth']= player_list_df['Date of birth'].apply(str)
player_list_df['Date of birth']= player_list_df['Date of birth'].apply(lambda x: re.sub("[( )].*","",x))
player_list_df['Foot']= player_list_df['Foot'].apply(lambda x:transform_foot(str(x)))
player_list_df['key']= player_list_df["name"].apply(lambda x:x.replace(" ",""))+"_"+player_list_df['Height']+"_"+player_list_df['Date of birth']
clb_list_df=pd.DataFrame(clb_list)

# CUD_CLB(clb_list_df)
# with pd.ExcelWriter('output.xlsx') as writer:  
#     clb_list_df.to_excel(writer, sheet_name='CLB')
#     player_list_df.to_excel(writer, sheet_name='Player')

# CUD_Player(player_list_df)
# CUD_Player_Properties(player_list_df)
# CUD_Player_Club(player_list_df)

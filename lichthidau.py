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
from datetime import datetime

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

def transform_day(str):
    day_list=str.split("/")
    str= "20"+day_list[2]+"/"+day_list[0]+"/"+day_list[1]
    return str

def transform_time(mystr):
    mystr= mystr.replace(" ","")
    mystr=mystr.replace("\n","")
    mystr=mystr.replace("\t","")
    if mystr == "":
        return None
    if mystr[1] == ":":
        mystr="0"+mystr
    if "PM" in mystr or "pm" in mystr:
        mystr=mystr.replace("PM","")
        mystr=mystr.replace("pm","")
        minute= mystr[len(mystr)-2]+mystr[len(mystr)-1]
        hour= mystr[0]+mystr[1]
        mystr= str(int(hour)+12)+":"+str(minute)
    mystr=mystr.replace("AM","")
    mystr=mystr.replace("am","")
    return mystr

def transform_result(mystr):
    return mystr

def transform_height(str):
    str=str.replace(",","")
    str=str.replace("m","")
    char=u'\xa0'
    str=str.replace(char,"")
    return str

headers = {"User-Agent":"Mozilla/5.0"}
my_request = requests.get('https://www.transfermarkt.com/v-league-1/gesamtspielplan/wettbewerb/VIE1/saison_id/2020', verify=False,headers=headers)
my_soup = BeautifulSoup(my_request.text, "lxml")

def is_all_thread_done():
    count=0
    while(1):
        if threading.active_count() ==1:
            print("Everything done")
            break
        count+=1
        print("Have "+str(threading.active_count())+" thread working============================"+str(count))
        time.sleep(4)

def CUD_Player(player_key,player_international_name,player_country_name,player_img_url,player_birthday,player_height,player_foot,player_nationality):
    connection=getConnection()
    try:
        query="insert into Player values (?,?,?,?,?,?,?,?)"
        cursor=connection.cursor()
        cursor.execute(query,[player_key,player_international_name,player_country_name,player_img_url,player_birthday,player_height,player_foot,player_nationality,])
        print("Insert success")
        connection.commit()
    except Exception as e:
        connection.rollback()
        print("Insert Error")
        print(e)

def CUD_Player_Properties(player_key,start_date,property_name,property_value):
    connection=getConnection()
    try:
        query="insert into Player_Properties values (?,?,?,?)"
        cursor=connection.cursor()
        cursor.execute(query,[player_key,start_date,property_name,property_value])
        print("Insert success")
        connection.commit()
    except Exception as e:
        connection.rollback()
        print("Insert Error")
        print(e)

def CUD_Player_Club(player_key,club_name,start_time, end_time,transfer_money):
    connection=getConnection()
    try:
        query="insert into Player_Clubs values (?,?,?,?,?)"
        cursor=connection.cursor()
        cursor.execute(query,[player_key,club_name,start_time,end_time,transfer_money,])
        print("Insert success")
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(query)
        print("Insert Error")
        print(e)

def CUD_Player_Match_Event(player_key,match_key,event_name,start_time):
    connection=getConnection()
    try:
        query="insert into Player_Match_Event values (?,?,?,?)"
        cursor=connection.cursor()
        cursor.execute(query,[player_key,match_key,event_name,start_time])
        print("Insert success")
        connection.commit()
    except Exception as e:
        connection.rollback()
        print("Insert Error")
        print(e)

def CUD_Match(match_id,match_happen_time, match_location, clb_home_name, clb_away_name, match_desscription, match_result):
    connection=getConnection()
    try:
        query="insert into Match values (?,?,?,?,?,?,?)"
        cursor=connection.cursor()
        cursor.execute(query,[match_id,match_happen_time, match_location, clb_home_name, clb_away_name, match_desscription, match_result,])
        print("Insert success")
        connection.commit()
    except Exception as e:
        connection.rollback()
        print("Insert Match Error")
        print(e)

def clb_is_in_V(clb_name):
    connection= getConnection()
    cursor=connection.cursor()
    try:
        cursor.execute("select clb_name from CLB where clb_name=?",[clb_name,])
    except Exception as e:
        print("R went wrong!!")
    records= cursor.fetchall()
    if len(records) == 0:
        connection.close()
        return False
    else:
        connection.close()
        return True

def is_exist_player(player_key):
    connection= getConnection()
    cursor=connection.cursor()
    try:
        cursor.execute("select player_id from Player where player_id=?",[player_key,])
    except Exception as e:
        print("R went wrong!!")
    records= cursor.fetchall()
    if len(records) != 0:
        connection.close()
        return True                
    else:
        connection.close()
        return False

def redirect_profile(player_stat_url):
    player_goal_request= requests.get("https://www.transfermarkt.com"+ player_stat_url, verify=False,headers=headers)
    player_goal_soup= BeautifulSoup(player_goal_request.text, "lxml")
    player_url= player_goal_soup.select_one("#submenu #profile a")['href']
    return player_url

def handling_player(player_url):
    player_request= requests.get("https://www.transfermarkt.com"+ player_url, verify=False,headers=headers)
    player_soup= BeautifulSoup(player_request.text, "lxml")
    player_name= player_soup.select_one('.dataName h1').text
    player_name= player_name.replace(" ","")

    detail_info_el_tr_list= player_soup.select('table.auflistung tr')
    player_dic={}
    for el in detail_info_el_tr_list:
        key= el.select_one('th').text.strip().replace(":","")
        value= el.select_one('td').text.strip()
        player_dic[key]=value

    if "Height" in player_dic:
        player_height= player_dic['Height']
        player_height= transform_height(player_height)
    else :
        player_height= 0
    
    if "Date of birth" in player_dic:
        player_birthday= player_dic['Date of birth']
        player_birthday= player_birthday.replace(" Happy Birthday","")
        player_birthday= player_birthday.replace(",","").replace(" ","-")
        myformat = datetime.strptime(player_birthday, "%b-%d-%Y")
        player_birthday= myformat.strftime('%Y-%m-%d')
    else:
        player_birthday="2222-11-11"
    
    player_key=player_name+"_"+str(player_height)+"_"+player_birthday

    if not is_exist_player(player_key):
        print("----")
        print("Không có cầu thủ " + player_key)
        print("----")

        # Kiểm tra clb ở V-leauge ko
        clb_url= player_soup.select_one('.dataZusatzbox .dataZusatzDaten a')['href']
        clb_request= requests.get("https://www.transfermarkt.com"+ clb_url, verify=False,headers=headers)
        clb_soup= BeautifulSoup(clb_request.text, "lxml")
        if clb_soup.select_one('.dataHeader h1 span') == None:
            clb_name= "OTHER"
        else:
            clb_name= clb_soup.select_one('.dataHeader h1 span').text
            if not clb_is_in_V(clb_name):
                clb_name="OTHER"
        
        if 'Name in home country' in player_dic: 
            name_in_country=player_dic['Name in home country']
            if not name_in_country:
                name_in_country='noinfo'
        else:
            name_in_country='noinfo'

        if 'Foot' in player_dic:
            player_foot= player_dic['Foot']
            if not player_dic['Foot']:
                player_foot='noinfo'
        else:
            player_foot='noinfo'

        if 'Citizenship' in player_dic:
            player_country=player_dic['Citizenship']
            if player_dic['Citizenship']=="":
                player_country='noinfo'
        else:
            player_country='noinfo'

        if 'Position' in player_dic:
            player_position= player_dic['Position']
            if player_position=="":
                player_position="noinfo"
        else:
            player_position="noinfo"

        player_img_url= player_soup.select_one('.dataBild img')['src']
        if player_soup.select_one('.dataName span') == None:
            player_number= None
        else:
            player_number= player_soup.select_one('.dataName span').text.replace("#","")                                                                              
    
        #Thêm cầu thủ vào DB
        CUD_Player(player_key,player_name,name_in_country,player_img_url,player_birthday,player_height,player_foot,player_country)
        CUD_Player_Club(player_key,clb_name,'2021-05-23',None,None)
        CUD_Player_Properties(player_key,'2021-05-23','number',player_number)
        CUD_Player_Properties(player_key,'2021-05-23','position',player_position)
        CUD_Player_Properties(player_key,'2021-05-23','performance',3)
        CUD_Player_Properties(player_key,'2021-05-23','value',0)   

    return player_key

def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)

tempday=""
temptime=""
def handling_match(matchday_el):
    # GET từng row trận đấu
    if not matchday_el.has_attr('class'):
        # GET từng column từng row
        td_el_list= matchday_el.select('td')
        # GET ngày và thời gian trận đấu
        if td_el_list[0].select_one('a') != None:
            match_day= td_el_list[0].select_one('a').text
            match_day= transform_day(match_day)
            global tempday
            tempday=match_day
        else:
            match_day=tempday
        
        if td_el_list[1] != None:
            match_time= td_el_list[1].text
            match_time=match_time.strip()

            if hasNumbers(match_time):
                match_time=transform_time(match_time)
                global temptime
                temptime=match_time
            else:
                match_time=temptime
        
        # GET kết quả trận đấu
        match_result= td_el_list[4].select_one('a').text
        match_result= transform_result(match_result)

        # GET chi tiết trận đấu
        match_detail_url= td_el_list[4].select_one('a')['href']
        match_detail_request= requests.get("https://www.transfermarkt.com"+ match_detail_url, verify=False,headers=headers)
        match_detail_soup= BeautifulSoup(match_detail_request.text, "lxml")

        # GET tên đội nhà và đội khách
        clb_name= match_detail_soup.select('.box .sb-team a.sb-vereinslink')
        home_clb_name= clb_name[0].text
        away_clb_name= clb_name[1].text

        match_year= "2021"
        match_key= match_year + "_"+ home_clb_name + "_"+ away_clb_name
        print(match_key)
        match_hour= match_time.split(":")[0]
        if int(match_hour) == 24:
            match_time= "23:59"

        # tạo trận dấu ========
        CUD_Match(match_key,match_day+" "+ match_time,None,home_clb_name,away_clb_name,None,match_result)

        if not match_result == "-:-":
            clb_detail_el_list= match_detail_soup.select('.box .large-6.columns')

            home_detail_el_list= clb_detail_el_list[0].select('.aufstellung-spieler-container')
            home_main_lineup_list=[]

            # Từng player trong đội hình chính
            for el in home_detail_el_list:
                player_url= el.select_one('a')['href']
                player_key= handling_player(player_url)          
                # sau khi xử lí cầu thủ
                # add cầu thủ vào list
                home_main_lineup_list.append(player_key)

            away_detail_el_list= clb_detail_el_list[1].select('.aufstellung-spieler-container')
            away_main_lineup_list=[]
            for el in away_detail_el_list:
                player_url= el.select_one('a')['href']
                player_key= handling_player(player_url)
                away_main_lineup_list.append(player_key)

            # Từng player trong đội hình sub
            home_sub_lineup_list=[]
            home_sub_detail_el_list= clb_detail_el_list[0].select('.large-5.columns.small-12.aufstellung-ersatzbank-box.aufstellung-vereinsseite tr')
            for el in home_sub_detail_el_list:
                if home_sub_detail_el_list.index(el) == len(home_sub_detail_el_list)-1:
                    continue
                player_url= el.select_one('a')['href']
                player_key= handling_player(player_url)
                home_sub_lineup_list.append(player_key)
            
            away_sub_lineup_list=[]
            away_sub_detail_el_list= clb_detail_el_list[1].select('.large-5.columns.small-12.aufstellung-ersatzbank-box.aufstellung-vereinsseite tr')
            for el in away_sub_detail_el_list:
                if away_sub_detail_el_list.index(el) == len(away_sub_detail_el_list)-1:
                    continue
                player_url= el.select_one('a')['href']
                player_key= handling_player(player_url)
                away_sub_lineup_list.append(player_key)

         
            print("===== đã xong gia đoạn cầu thủ")

            goal_player_list=[]
            assist_player_list=[]
            player_in_list=[]
            player_out_list=[]
            redcard_player_list=[]
            yellowcard_player_list=[]

            goal_el_section= match_detail_soup.select_one('#sb-tore')
            if goal_el_section != None:
                goal_el_list= goal_el_section.select('ul li.sb-aktion-heim')
                if goal_el_list != None:
                    for goal_el in goal_el_list:
                        time_el= goal_el.select_one('.sb-aktion-uhr span')['style']
                        position_list=time_el.replace(";","").replace("px","").replace("-","").split(" ")
                        event_time= (int(position_list[2])/36)*10 + (int(position_list[1])/36+1)
                    
                        player_el_list= goal_el.select('.sb-aktion-aktion a')
                        player_goal_url= player_el_list[0]['href']
                        player_goal_url= redirect_profile(player_goal_url)
                        player_goal_key= handling_player(player_goal_url)

                        goal_player_dic={}
                        goal_player_dic['player']=player_goal_key
                        goal_player_dic['time']= event_time
                        goal_player_list.append(goal_player_dic)

                        if len(player_el_list) == 2:
                            player_assist_url= player_el_list[1]['href']
                            player_assist_url= redirect_profile(player_assist_url)
                            player_assist_key= handling_player(player_assist_url)

                            assist_player_dic={}
                            assist_player_dic['player']=player_assist_key
                            assist_player_dic['time']= event_time
                            assist_player_list.append(assist_player_dic)

            print("xong phan ghi ban")
            sub_el_section = match_detail_soup.select_one('#sb-wechsel')
            if sub_el_section != None:
                sub_el_list= sub_el_section.select('ul li .sb-aktion')
                if sub_el_list != None:
                    for sub_el in sub_el_list:
                        time_el= sub_el.select_one('.sb-aktion-uhr span')['style']
                        position_list=time_el.replace(";","").replace("px","").replace("-","").split(" ")
                        event_time= (int(position_list[2])/36)*10 + (int(position_list[1])/36+1)

                        player_in_url= sub_el.select_one('.sb-aktion-wechsel-ein a')['href']
                        player_in_url= redirect_profile(player_in_url)
                        player_in_key= handling_player(player_in_url)

                        player_in_dic={}
                        player_in_dic['player']= player_in_key
                        player_in_dic['time']= event_time

                        player_out_url= sub_el.select_one('.sb-aktion-wechsel-aus a')['href']
                        player_out_url= redirect_profile(player_out_url)
                        player_out_key= handling_player(player_out_url)

                        player_out_dic={}
                        player_out_dic['player']= player_out_key
                        player_out_dic['time']= event_time

                        player_in_list.append(player_in_dic)
                        player_out_list.append(player_out_dic)

            print("xong phan doi nguoi")
            card_el_section= match_detail_soup.select_one('#sb-karten')
            if card_el_section != None:
                card_el_list= card_el_section.select('ul li .sb-aktion')
                if card_el_list != None:
                    for card_el in card_el_list:
                        time_el= card_el.select_one('.sb-aktion-uhr span')['style']
                        position_list=time_el.replace(";","").replace("px","").replace("-","").split(" ")
                        event_time= (int(position_list[2])/36)*10 + (int(position_list[1])/36+1)
                        
                        foul_player_url= card_el.select_one('.sb-aktion-aktion a')['href']
                        foul_player_url= redirect_profile(foul_player_url)
                        foul_player_key= handling_player(foul_player_url)

                        card_det_el= card_el.select_one('.sb-aktion-spielstand span')
                        if "sb-rot" in card_det_el['class']:
                            card_type= "red"
                            foul_dic={}
                            foul_dic['player']=foul_player_key
                            foul_dic['time']= event_time
                            redcard_player_list.append(foul_dic)
                        elif "sb-gelb" in card_det_el['class']:
                            card_type= "yellow"
                            foul_dic={}
                            foul_dic['player']=foul_player_key
                            foul_dic['time']= event_time
                            yellowcard_player_list.append(foul_dic)                
            print("xong phan the")
            # add event match
            # event ra san
            try:
                for player in home_main_lineup_list:
                    CUD_Player_Match_Event(player,match_key,"main",0)
                for player in away_main_lineup_list:
                    CUD_Player_Match_Event(player,match_key,"main",0)
            except Exception as e:
                print(e)
                print("Lỗi đội hình ra sân")
            #event du bi
            try:
                for player in home_sub_lineup_list:
                    CUD_Player_Match_Event(player,match_key,"sub",0)
                for player in away_sub_lineup_list:
                    CUD_Player_Match_Event(player,match_key,"sub",0)
            except Exception as e:
                print(e)
                print("Lỗi đội hình dự bị")
            #event ghi ban
            try:
                for player in goal_player_list:
                    CUD_Player_Match_Event(player['player'],match_key,"goal",int(player['time']))
                for player in assist_player_list:
                    CUD_Player_Match_Event(player['player'],match_key,"assist",int(player['time']))
            except Exception as e:
                print(e)
                print("Lỗi ghi bàn hoặc kiến tạo")

            # event the
            try:        
                for player in redcard_player_list:
                    CUD_Player_Match_Event(player['player'],match_key,"red",int(player['time']))
                for player in yellowcard_player_list:
                    CUD_Player_Match_Event(player['player'],match_key,"yellow",int(player['time']))
            except Exception as e:
                print(e)
                print("Lỗi thẻ")

            # event doi cau thu
            try:        
                for player in player_in_list:
                    CUD_Player_Match_Event(player['player'],match_key,"in",int(player['time']))
                for player in player_out_list:
                    CUD_Player_Match_Event(player['player'],match_key,"out",int(player['time']))
            except Exception as e:
                print(e)
                print("Lỗi thay đổi cầu thủ trong trận đấu")

class Matchday_Thread(Thread):
    def __init__(self,matchday_table_el):
        super(Matchday_Thread,self).__init__()
        self.matchday_table_el=matchday_table_el
    def run(self):  
        Get_Match_Data(self.matchday_table_el)

class Match_Thread(Thread):
    def __init__(self,matchday_el):
        super(Match_Thread,self).__init__()
        self.matchday_el= matchday_el
    def run(self):
        handling_match(self.matchday_el)


def Get_Match_Data(matchday_table_el):
    matchday_el_list=matchday_table_el.select('tbody >tr')
    # for matchday_el in matchday_el_list:
    #     print("---------new---")
    #     handling_match(matchday_el)

    for matchday_el in matchday_el_list:
        try:
            el_thread= Match_Thread(matchday_el)
            el_thread.start()
        except Exception as e:
            print(e)
            print("Thread Errror")
    

matchday_table_el_list= my_soup.select('.row .large-6.columns')
Get_Match_Data(matchday_table_el_list[13])

# for matchday_table_el in matchday_table_el_list:
#     try:
#         if matchday_table_el_list.index(matchday_table_el)==0:
#             continue
#         el_thread= Matchday_Thread(matchday_table_el)
#         el_thread.start()
#     except:
#         print("Thread Error")

is_all_thread_done()

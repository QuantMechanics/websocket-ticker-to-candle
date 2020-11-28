# -*- coding: utf-8 -*-
"""
Quant Mechanics

@author: Quant Mechanics (https://twitter.com/quant_mechanics)
"""
import atexit
import time
import sqlite3
import logging
from alice_blue import *
import json
import datetime
from systemutils import util
import os
from pathlib import Path
from threading import Thread
from sqlalchemy import create_engine, pool
import pandas as pd
from systemutils import tel_msgr
logging.basicConfig(level=logging.INFO)

date_time_now = datetime.datetime.now()
socket_opened = False
base_path = util.get_project_root()
print(base_path)
db_file_path = os.path.join(base_path,  "data", "stock_data_db.db")
tel_utl = tel_msgr.TelUtil()


class clSocketRunner(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.symbol_query = "select * from stock_symbol;"
        self.daily_token_file = os.path.join(
            base_path, "credentials", "access_tokens.json")
        self.new_mysql_table_query = "CREATE TABLE IF NOT EXISTS `{0}` (\
                                    `ts` datetime NOT NULL , \
                                    `open` double DEFAULT NULL,\
                                    `high` double DEFAULT NULL,\
                                    `low` double DEFAULT NULL,\
                                    `close` double DEFAULT NULL,\
                                    `volume` bigint DEFAULT NULL,\
                                    PRIMARY KEY (`ts`)\
                                    )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
        self.system_cred = self.loadJson(os.path.join(
            base_path, "credentials", "env_credential.json"))
        self.msql_engine = create_engine("mysql+pymysql://{user}:{pw}@{endpoint}/{db}"
                                         .format(endpoint=self.system_cred["mysql"]["endpoint"],
                                                 user=self.system_cred["mysql"]["username"],
                                                 pw=self.system_cred["mysql"]["password"],
                                                 db=self.system_cred["mysql"]["dbschema"]))
        self.lite_conn = sqlite3.connect(
            db_file_path, check_same_thread=False, timeout=10, isolation_level="DEFERRED")
        self.c = self.lite_conn.cursor()
        self.lite_conn.execute("PRAGMA journal_mode = WAL")
        self.lite_conn.execute("PRAGMA cache_size=10000")
        self.lite_conn2 = sqlite3.connect(
            db_file_path, check_same_thread=False, timeout=10, isolation_level="DEFERRED")
        self.c2 = self.lite_conn2.cursor()
        self.lite_conn2.execute("PRAGMA journal_mode = WAL")
        self.lite_conn2.execute("PRAGMA cache_size=10000")
        # This is listening start time
        self.starting_time = int(9) * 60 + int(14)
        # this is the listening stop time
        self.closing_minutes = int(16) * 60 + int(17)
        self.alice_access_token = self.doGetTokenforToday()
        self.alice = self.getAliceBlueObj()
        self.mysql_do_exist = self.create_new_scrip_tables()
        atexit.register(self.cleanup)
        print("1> Socket_runner Job execution begun at {0}".format(
            datetime.datetime.now()))
        tel_utl.sendMsg("1> Socket_runner  Job execution begun at {0}".format(
            datetime.datetime.now()))

    def loadJson(self, filename=""):  # This helps in reading JSON files
        with open(filename, mode="r") as json_file:
            data = json.load(json_file)
            return data

    # create table in mysql if it doesn't exists
    def create_new_scrip_tables(self):
        tempres = self.c.execute(self.symbol_query)
        rows = tempres.fetchall()
        for row in rows:
            self.msql_engine.execute(self.new_mysql_table_query.format(row[1]))
            time.sleep(0.5)

    def cleanup(self):  # Dont leave your footprints dispose whats not needed after job is done
        self.lite_conn.execute("delete from stocks_websocket;")
        tel_utl.sendMsg("cleanup> Socket_runner Job execution ended at {0}".format(
            datetime.datetime.now()))
        self.lite_conn.close()
        self.msql_engine.dispose()
        print("doing cleanup")
        print("Job execution ended at {0}".format(datetime.datetime.now()))

    # Gives either latest or taken saved token for the day.
    def doGetTokenforToday(self):
        cred_data = self.system_cred
        daily_token_file = self.daily_token_file
        # print(type(cred_data))

        if(os.path.exists(daily_token_file)):
            data = self.loadJson(daily_token_file)
            if "access_token" in data and data["date"] == date_time_now.strftime("%Y-%m-%d"):
                return data["access_token"]
            else:
                access_token = AliceBlue.login_and_get_access_token(
                    username=cred_data["alice"]["username"], password=cred_data["alice"]["password"], twoFA=cred_data["alice"]["twoFA"],  api_secret=cred_data["alice"]["api_secret"])
                json_data = {"access_token": access_token,
                             "date": date_time_now.strftime("%Y-%m-%d"),
                             "broker": "alice_blue"
                             }
                with open(daily_token_file, "w") as outfile:
                    json.dump(json_data, outfile)
                    return access_token
        else:
            access_token = AliceBlue.login_and_get_access_token(
                username=cred_data["alice"]["username"], password=cred_data["alice"]["password"], twoFA=cred_data["alice"]["twoFA"],  api_secret=cred_data["alice"]["api_secret"])
            json_dataa = {"access_token": access_token,
                          "date": date_time_now.strftime("%Y-%m-%d"),
                          "broker": "alice_blue"}
            with open(daily_token_file, "w") as outfile:
                json.dump(json_dataa, outfile)
                return access_token

    def getAliceBlueObj(self):
        cred_data = self.system_cred
        access_token = self.alice_access_token
        alice_obj = AliceBlue(username=cred_data["alice"]["username"], password=cred_data["alice"]["password"],
                              access_token=access_token, master_contracts_to_download=cred_data["alice"]["contracts"])
        return alice_obj

    def insert_ticks(self, message):
        vals = [message["exchange"], message["ltp"], message["volume"], message["instrument"][2], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(
            message["exchange_time_stamp"]))]
        query = "INSERT INTO stocks_websocket(exchange,price,volume,symbol,ts) VALUES (?,?,?,?,?)"
        with self.lite_conn as conn:
            conn.execute(query, vals)

    def event_handler_quote_update(self, message):
        self.insert_ticks(message)

    def open_callback(self):
        # print('socket opened')
        global socket_opened
        socket_opened = True

    def run(self):
        # print("run")
        global socket_opened
        current_time_minutes = datetime.datetime.now().hour * 60 + \
            datetime.datetime.now().minute
        if(current_time_minutes < self.closing_minutes):
            self.alice.start_websocket(subscribe_callback=self.event_handler_quote_update,
                                       socket_open_callback=self.open_callback,
                                       run_in_background=True)

            while(socket_opened == False):
                pass
            tempres = self.c.execute(self.symbol_query)
            rows = tempres.fetchall()
            symbol_list = []
            for row in rows:
                # print(row)
                symbol_list.append(
                    self.alice.get_instrument_by_symbol('NSE', row[1]))
            # print(symbol_list)
            self.alice.subscribe(symbol_list, LiveFeedType.COMPACT)
            try:
                while True:
                    if (current_time_minutes > starting_time and current_time_minutes <= self.closing_minutes):
                        # print("lets wait for 15min=900")
                        time.sleep(int(15)*60)
                    else:
                        print("run: Market has closed. {0}".format(
                            datetime.datetime.now()))
                        break
            except Exception as e:
                print(e)
            finally:
                pass
        else:
            print("Market is closed. {0}".format(datetime.datetime.now()))

    def resampling_function(self):
        # print("resampling_function")
        while True:
            current_time_minutes = datetime.datetime.now().hour * 60 + \
                datetime.datetime.now().minute
            if (current_time_minutes > self.starting_time and current_time_minutes <= self.closing_minutes):
                sec_now = datetime.datetime.now().second
                if(sec_now <= 5):
                    # print("in if "+str(sec_now))
                    tesmp_res = self.c2.execute(self.symbol_query)
                    try:
                        self.lite_conn.commit()
                    except:
                        pass
                    rows = tesmp_res.fetchall()
                    for one_row in rows:
                        # print(one_row[1])
                        df = pd.read_sql_query(
                            "select * from stocks_websocket where symbol='{0}'".format(one_row[1]), self.lite_conn, index_col="ts", parse_dates=True)
                        if len(df) > 0:
                            df.index = pd.to_datetime(df.index)
                            df_resample = df["price"].resample("1min").ohlc()
                            df_resample["volume"] = df["volume"].resample(
                                '1min').sum().astype(int)
                            df_resample.dropna(inplace=True)
                            # print(df_resample)
                            lst = df_resample.index.astype(str).tolist()
                            joined_string = "('"+"','".join(lst)+"')"
                            self.msql_engine.execute(
                                "DELETE FROM `{a}` WHERE ts in {b}".format(a=one_row[1], b=joined_string))
                            # # print("DELETE FROM `{a}` WHERE ts in {b}".format(
                            #     a=one_row[1], b=joined_string))
                            df_resample.to_sql(one_row[1], con=self.msql_engine,
                                               if_exists='append', chunksize=1000, method='multi')
                            with self.lite_conn2 as conn:
                                # self.lite_conn2.commit()
                                conn.execute(
                                    "delete from stocks_websocket where symbol='{sym}'".format(sym=one_row[1]))
                                try:
                                    conn.commit()
                                except:
                                    pass

                    # time.sleep(10)
                else:
                    # print("else"+str(sec_now))
                    time.sleep(59-sec_now)
            else:
                print("Outside trading hours : {0}".format(
                    datetime.datetime.now()))
                break


current_time_minutes = datetime.datetime.now().hour * 60 + \
    datetime.datetime.now().minute
starting_time = int(9) * 60 + int(14)

if(current_time_minutes < starting_time):
    print("Current time :{0}. Market yet to open will wait in queue".format(
        datetime.datetime.now()))
    time.sleep((starting_time-current_time_minutes)*60)


cl = clSocketRunner()
all_processes = []
socket_thread = Thread(target=cl.run, name="socket_thread")
resampling_thread = Thread(
    target=cl.resampling_function, name="resampling_thread")
all_processes.append(socket_thread)
all_processes.append(resampling_thread)


socket_thread.start()
resampling_thread.start()

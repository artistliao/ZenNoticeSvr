
# -*- coding:utf-8 -*-
#! python3

import time
import datetime
import threading
import sys
import os
import requests
import mysql.connector
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from queue import Queue,LifoQueue,PriorityQueue

import configparser

from zd_logging import g_logger
from utils import *


#从mysql中读取数据类
class ZenNoticeData:
    mydb = None
    connect_info = None
    engine = None
    dingtalkQueue = Queue()
    drawQueue = Queue()
    tradeQueue = Queue()

    def __init__(self, path):
        if path=='':
            g_logger.info("ZenNoticeData init, path is null!")

        try:
            g_logger.info('cfg_path=%s', path)
            cf = configparser.ConfigParser()
            cf.read(path)
            host = cf.get("mysql", "host")
            username = cf.get("mysql", "username")
            password = cf.get("mysql", "password")
            dbname = cf.get("mysql", "dbname")
            port = cf.get("mysql", "port")
            self.mydb = mysql.connector.connect(
                host=host,
                port=port,
                user=username,
                passwd=password,
                database=dbname
            )
            self.connect_info = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(username, password, host, port, dbname) #1
            self.engine = create_engine(self.connect_info)
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)

    def __del__(self):
        self.mydb.close()

    def PutDingtalk(self, msg):
        g_logger.info('PutDingtalk msg=%s', msg)
        self.dingtalkQueue.put(msg)

    def PutDraw(self, draw_info):
        g_logger.info('PutDraw draw_info=%s', str(draw_info))
        self.drawQueue.put(draw_info)

    def PutTrade(self, trade_info):
        g_logger.info('PutTrade trade_info=%s', str(trade_info))
        self.tradeQueue.put(trade_info)

    def DingtalkHandle(self):
        while True:
            qsize = self.dingtalkQueue.qsize()
            if (qsize==0):
                break
            msg = self.dingtalkQueue.get()
            self.SendDingDing(msg)

    def SendDingDing(self, text):
        ding_url = "https://oapi.dingtalk.com/robot/send?access_token=xxxxxx"
        msg = dict()
        msg["msgtype"] = "text"
        msg["text"] = dict()
        msg["text"]["content"] = text
        msg["at"] = dict()
        msg["at"]["isAtAll"] = True
        r = requests.post(ding_url, json=msg, timeout=5)
        if r.status_code != 200:
            g_logger.error("sendDingDing error. code:%d.", r.status_code)
        else:
            g_logger.info("sendDingDing succ. text:%s", r.text)

    def DrawHandle(self):
        while True:
            qsize = self.drawQueue.qsize()
            if (qsize==0):
                break
            draw_info  = self.drawQueue.get()
            self.InsertDrawInfo(draw_info)

    #插入DrawInfo
    def InsertDrawInfo(self, draw_info):
        g_logger.info('InsertDrawInfo draw_info=%s', str(draw_info))
        mycursor = self.mydb.cursor()

        try:
            str_sql = "INSERT INTO trade.draw_info(code, sec_type, name, peroid, kline_start_ts, ext_info, status) VALUES ('"
            str_sql += draw_info['code']
            str_sql += "','"
            str_sql += str(draw_info['sec_type'])
            str_sql += "','"
            str_sql += draw_info['name']
            str_sql += "','"
            str_sql += draw_info['period']
            str_sql += "','"
            str_sql += str(draw_info['kline_start_ts'])
            str_sql += "','"
            str_sql += str(draw_info['ext_info'])
            str_sql += "', '0')"

            try:
                mycursor.execute(str_sql)
                self.mydb.commit()
            except Exception as e:
                self. mydb.rollback()
                g_logger.warning("str_sql=%s", str_sql)
                g_logger.warning(str(e))
                g_logger.exception(e)
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)
            return -1

        return 0


def CheckSameDay(ts1, ts2):
    ts1_zero = ts1 - (ts1+28800)%86400
    ts2_zero = ts2 - (ts2+28800)%86400
    if (ts1_zero==ts2_zero):
        return True
    else:
        return False

def DayZeroTs(ts1):
    return ts1 - (ts1+28800)%86400

if __name__ == "__main__":
    zen_notice_data = ZenNoticeData('config.ini')

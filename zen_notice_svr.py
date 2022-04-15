
# -*- coding:utf-8 -*-
#! python3


import time
import sys
import os
import mysql.connector

from zd_logging import g_logger
from zen_notice_data import *
from utils import *

from concurrent import futures
import time
import random
import grpc
import zen_notice_pb2
import zen_notice_pb2_grpc

#读取数据
g_zen_notice_data = ZenNoticeData( 'config.ini')

# 实现 proto 文件中定义的 ZenNoticeHandleServicer
class ZenNoticeHandle(zen_notice_pb2_grpc.ZenNoticeHandleServicer):
    # 实现 proto 文件中定义的 rpc 调用
    def NoticeLine(self, request, context):
        g_logger.info("NoticeLine start")
        g_logger.info("NoticeLine code:%s, name:%s, trigger_time:%s, direction:%d, line_start_time:%s, line_end_time:%s, high:%f, low:%f, kline_start_time:%s",
                      request.code, request.name, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.trigger_ts))+'('+ str(request.trigger_ts) +')',
                      request.direction, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.line_start_ts))+'('+ str(request.line_start_ts) +')',
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.line_end_ts)) +'('+ str(request.line_end_ts) +')',
                      request.high, request.low, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.kline_start_ts)) +'('+ str(request.kline_start_ts) +')',)

        dt_msg = 'code:' + request.code + '\n'
        dt_msg += 'sec_type:' + request.sec_type + '\n'
        dt_msg += 'name:' + request.name + '\n'
        dt_msg += 'trigger_time:' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.trigger_ts)) + '\n'
        dt_msg += 'direction:' + str(request.direction) + '\n'
        dt_msg += 'start_time:' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.line_start_ts)) + '\n'
        dt_msg += 'end_time:' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.line_end_ts)) + '\n'
        if request.direction==1:
            dt_msg += 'low:' + str(round(request.low, 2)) + '\n'
            dt_msg += 'high:' + str(round(request.high, 2)) + '\n'
        else:
            dt_msg += 'high:' + str(round(request.high, 2)) + '\n'
            dt_msg += 'low:' + str(round(request.low, 2)) + '\n'

        dt_msg += 'kline_start_time:' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.kline_start_ts)) + '\n'

        g_zen_notice_data.PutDingtalk(dt_msg)

        draw_info = dict()
        draw_info['code'] = request.code
        draw_info['sec_type'] = request.sec_type
        draw_info['name'] = request.name
        draw_info['period'] = request.period
        draw_info['kline_start_ts'] = request.kline_start_ts
        g_zen_notice_data.PutDraw(draw_info)
        return zen_notice_pb2.NoticeLineRsp(status = 0, message="success")

    def NoticeDraw(self, request, context):
        g_logger.info("NoticeDraw code:%s, sec_type:%s, name:%s, period:%s ",
                      request.code, request.sec_type, request.name, request.period)

        draw_info = dict()
        draw_info['code'] = request.code
        draw_info['sec_type'] = request.sec_type
        draw_info['name'] = request.name
        draw_info['period'] = request.period
        draw_info['kline_start_ts'] = request.kline_start_ts
        g_zen_notice_data.PutDraw(draw_info)
        return zen_notice_pb2.NoticeDrawRsp(status = 0, message="success")

    def NoticeTrade(self, request, context):
        g_logger.info("NoticeTrade start")
        g_logger.info("NoticeTrade code:%s, name:%s, period:%s, trade_type:%d, open_cover_type:%d, "
                      "direction:%d, trigger_time:%s, kline_start_time:%s, price:%f, stop_loss_price:%f, ext_info:%s",
                      request.code, request.name, request.period, request.trade_type, request.open_cover_type, request.direction,
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.trigger_ts))+'('+ str(request.trigger_ts) +')',
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.kline_start_ts))+'('+ str(request.kline_start_ts) +')',
                      request.price, request.stop_loss_price, request.ext_info)

        dt_msg = 'code:' + request.code + '\n'
        dt_msg += 'sec_type:' + request.sec_type + '\n'
        dt_msg += 'name:' + request.name + '\n'
        if request.trade_type==1:
            dt_msg += 'trade_type:open\n'
        elif request.trade_type==2:
            dt_msg += 'trade_type:cover\n'
        dt_msg += 'open_cover_type:' + str(request.open_cover_type) + '\n'
        dt_msg += 'direction:' + str(request.direction) + '\n'
        dt_msg += 'trigger_time:' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.trigger_ts)) + '\n'
        dt_msg += 'kline_start_time:' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(request.kline_start_ts)) + '\n'
        dt_msg += 'price:' + str(round(request.price, 2)) + '\n'
        dt_msg += 'stop_loss_price:' + str(round(request.stop_loss_price, 2)) + '\n'

        g_zen_notice_data.PutDingtalk(dt_msg)

        draw_info = dict()
        draw_info['code'] = request.code
        draw_info['sec_type'] = request.sec_type
        draw_info['name'] = request.name
        draw_info['period'] = request.period
        draw_info['kline_start_ts'] = request.kline_start_ts
        draw_info['ext_info'] = request.ext_info
        g_zen_notice_data.PutDraw(draw_info)
        return zen_notice_pb2.NoticeLineRsp(status = 0, message="success")

class ZenNoticeSvr(threading.Thread):
    def __init__(self, threadname):
        threading.Thread.__init__(self, name='ZenNoticeSvr线程' + threadname)
        self.threadname = threadname

    def run(self):
        # 启动 rpc 服务
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
        zen_notice_pb2_grpc.add_ZenNoticeHandleServicer_to_server(ZenNoticeHandle(), server)
        server.add_insecure_port('[::]:18102')
        server.start()
        try:
            while True:
                time.sleep(60*60*24) # 1 day in seconds
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)
            server.stop(0)


if __name__ == "__main__":
    g_logger.info("zen_notice_svr start!")
    zen_notice_svr = ZenNoticeSvr('ZenNoticeSvr Thread')
    zen_notice_svr.start()

    while True:
        try:
            g_zen_notice_data.DingtalkHandle()
            g_zen_notice_data.DrawHandle()
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)
        time.sleep(1) # 1 day in seconds


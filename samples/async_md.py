#!/usr/bin/env python3
# encoding:utf-8
import json
import socket
import sys
import urllib.parse
from contextlib import closing
import time
from ctpwrapper import ApiStructure
from ctpwrapper import AsyncMdApiPy
import asyncio


def check_address_port(tcp):
    """
    :param tcp:
    :return:
    """
    host_schema = urllib.parse.urlparse(tcp)

    ip = host_schema.hostname
    port = host_schema.port

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex((ip, port)) == 0:
            return True  # OPEN
        else:
            return False  # closed


class AsyncMd(AsyncMdApiPy):
    """
    """

    def __init__(self, broker_id, investor_id, password, request_id=100):
        """
        """
        self.login = False
        self.broker_id = broker_id
        self.investor_id = investor_id
        self.password = password
        self._request_id = request_id

    @property
    def request_id(self):
        self._request_id += 1
        return self._request_id

    async def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        print("OnRspError:")
        print("requestID:", nRequestID)
        print(pRspInfo)
        print(bIsLast)

    async def OnFrontConnected(self):
        """
        :return:
        """
        user_login = ApiStructure.ReqUserLoginField(BrokerID=self.broker_id, UserID=self.investor_id, Password=self.password)
        self.ReqUserLogin(user_login, self.request_id)

    async def OnFrontDisconnected(self, nReason):
        print("Md OnFrontDisconnected {0}".format(nReason))
        sys.exit()

    async def OnHeartBeatWarning(self, nTimeLapse):
        """心跳超时警告。当长时间未收到报文时，该方法被调用。
        @param nTimeLapse 距离上次接收报文的时间
        """
        print('Md OnHeartBeatWarning, time = {0}'.format(nTimeLapse))

    async def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """
        用户登录应答
        :param pRspUserLogin:
        :param pRspInfo:
        :param nRequestID:
        :param bIsLast:
        :return:
        """
        print("OnRspUserLogin")
        print("requestID:", nRequestID)
        print("RspInfo:", pRspInfo)
        if self.async_wrapper:
            self.async_wrapper.on_rsp_user_login(pRspUserLogin, pRspInfo, nRequestID, bIsLast)
        # if pRspInfo.ErrorID != 0:
        #     print("RspInfo:", pRspInfo)
        # else:
        #     print("user login successfully")
        #     print("RspUserLogin:", pRspUserLogin)
        #     self.login = True

    async def OnRtnDepthMarketData(self, pDepthMarketData):
        """
        行情订阅推送信息
        :param pDepthMarketData:
        :return:
        """
        print("OnRtnDepthMarketData")
        print("DepthMarketData:", pDepthMarketData)
        if self.async_wrapper:
            self.async_wrapper.on_rtn_depth_market_data(pDepthMarketData)

    async def OnRspSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        """
        订阅行情应答
        :param pSpecificInstrument:
        :param pRspInfo:
        :param nRequestID:
        :param bIsLast:
        :return:
        """
        print("OnRspSubMarketData")
        print("RequestId:", nRequestID)
        print("isLast:", bIsLast)
        print("pRspInfo:", pRspInfo)
        print("pSpecificInstrument:", pSpecificInstrument)

    async def OnRspUnSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        """
        取消订阅行情应答
        :param pSpecificInstrument:
        :param pRspInfo:
        :param nRequestID:
        :param bIsLast:
        :return:
        """
        print("OnRspUnSubMarketData")
        print("RequestId:", nRequestID)
        print("isLast:", bIsLast)
        print("pRspInfo:", pRspInfo)
        print("pSpecificInstrument:", pSpecificInstrument)

async def main():
    json_file = open("config.json")
    config = json.load(json_file)
    json_file.close()

    investor_id = config["investor_id"]
    broker_id = config["broker_id"]
    password = config["password"]
    server = config["md_server"]
    if check_address_port(server):
        async_md = AsyncMd(broker_id, investor_id, password)
        async_md.Create()
        async_md.RegisterFront(server)
        async_md.Init()

        if async_md.login:
            async_md.SubscribeMarketData(["au2410"])
            asyncio.sleep(30)
            async_md.UnSubscribeMarketData(["au2410"])
            async_md.Join()


if __name__ == "__main__":
    asyncio.run(main())

# -*- coding: utf-8 -*-

import socket
import threading
import select
import re

from config import *

import logging

logging.basicConfig(level=logging.INFO,
                    filename=LOG_FILE,
                    filemode='a',
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

HEADLEN = 8


class ConnectionHandler:
    def __init__(self, connection, address, timeout, real_dbs):
        self.real_dbs = real_dbs
        self.real_db_host, self.real_db_port = real_dbs[0]['host'], real_dbs[0]['port']
        self.client = connection
        self.timeout = timeout
        self.method_def = {
            1: 'CONNECT',
            2: 'ACCEPT',
            3: 'ACK',
            4: 'REFUTE',
            5: 'REDIRECT',
            6: 'DATA',
            7: 'NULL',
            8: 'OTHER1',
            9: 'ABORT',
            10: 'OTHER2',
            11: 'RESEND',
            12: 'MARKER',
            13: 'ATTENTION',
            14: 'CONTROL',
        }

        self.method_map = {
            'CONNECT': self.__connect,
            'ACCEPT': self.__accept,
            'ACK': self.__ack,
            'REFUTE': self.__refute,
            'REDIRECT': self.__redirect,
            'DATA': self.__data,
            'NULL': self.__null,
            'OTHER1': self.__other1,
            'ABORT': self.__abort,
            'OTHER2': self.__other2,
            'RESEND': self.__resend,
            'MARKER': self.__marker,
            'ATTENTION': self.__attention,
            'CONTROL': self.__control,
            'NONE': self.__none,
        }

        self.redirect = False

        self.__forward()
        self.__close()

    def __close(self):
        self.client.close()
        self.real_db.close()

    def __connect_real_db(self):
        (soc_family, _, _, _, address) = socket.getaddrinfo(
            self.real_db_host, self.real_db_port)[0]  # 连接real db
        self.real_db = socket.socket(soc_family)
        self.real_db.connect(address)

    def __get_method(self, buf):
        if len(buf) >= 5 and self.method_def.get(buf[4]):
            self.method = self.method_def.get(buf[4])
        else:
            self.method = "NONE"
        return self.method

    def __get_buffer(self, soc):
        header = soc.recv(HEADLEN)
        bufferLen = header[0] * 256 + header[1] - \
            HEADLEN if len(header) >= 2 else 0
        data = header + soc.recv(bufferLen) if bufferLen > 0 else header
        return data

    def __send_buffer(self, soc, buf):
        soc.send(buf)

    # TNS protocol

    def __connect(self):
        """
        C -> S
        flag: 1
        """
        logging.info('CONNECT {}'.format(self.data))
        # 保存第一次连接时, 客户端的目标连接地址和端口. 以备REDIRECT时要用.
        self.connect_data = self.data
        matchObj = re.match(
            b'.*CONNECT_DATA.*\(HOST=([\.\d]+)\)\(PORT=(\d+)\).*', self.data, re.M)
        self.__send_buffer(self._out, self.data)

    def __accept(self):
        """
        S -> C
        flag: 2
        """
        self.__send_buffer(self._out, self.data)
        self.redirect_flag = False

    def __ack(self):
        """
        S -> C | C -> S
        flag: 3
        """
        self.__send_buffer(self._out, self.data)

    def __refute(self):
        """
        S -> C
        flag: 4
        """
        self.__send_buffer(self._out, self.data)

    def __redirect(self):
        """
        S -> C
        flag: 5
        """
        self.redirect_flag = True
        return

    def __data(self):
        """
        S -> C
        flag: 6
        """
        if self.redirect_flag:
            matchObj = re.match(
                b'.*\(HOST=([\.\d]+)\)\(PORT=(\d+)\).*DESCRIPTION.*', self.data, re.M)
            self.real_db_host = matchObj.group(1)
            self.real_db_port = matchObj.group(2)

            # 断开原来的连接
            self.real_db.close()
            # 连接新的服务器地址
            self.__connect_real_db()

            self.socs = [self.client, self.real_db]

            # 伪造一个client发的connect包.
            self.__send_buffer(self.real_db, self.connect_data)
        else:
            self.__send_buffer(self._out, self.data)

    def __null(self):
        """
        S -> C
        flag: 7
        """
        self.__send_buffer(self._out, self.data)

    def __other1(self):
        """
        S -> C
        flag: 8
        """
        self.__send_buffer(self._out, self.data)

    def __abort(self):
        """
        S -> C
        flag: 9
        """
        self.__send_buffer(self._out, self.data)

    def __other2(self):
        """
        S -> C
        flag: 10
        """
        self.__send_buffer(self._out, self.data)

    def __resend(self):
        """
        S -> C
        flag: 11
        """
        if self.redirect_flag:
            # 在redirect阶段. 直接给real db回伪造的connect包
            self.__send_buffer(self.real_db, self.connect_data)
        else:
            self.__send_buffer(self._out, self.data)

    def __marker(self):
        """
        S -> C
        flag: 12
        """
        self.__send_buffer(self._out, self.data)

    def __attention(self):
        """
        S -> C
        flag: 13
        """
        self.__send_buffer(self._out, self.data)

    def __control(self):
        """
        S -> C
        flag: 14
        """
        self.__send_buffer(self._out, self.data)

    def __none(self):
        """
        S -> C
        flag:
        """
        self.__send_buffer(self._out, self.data)

    def __forward(self):
        time_out_max = self.timeout / 2
        self.count = 0
        self.data = None

        # 默认先连接上第一个real_db
        self.__connect_real_db()
        self.socs = [self.client, self.real_db]
        self.connect_data = None   # 保存第一次connect. 用与redirect阶段, 伪造connect包
        self.redirect_select = True
        self.redirect_flag = False   # 重定向阶段, RESEND不要转给client.
        self.accept_flag = False
        self.method = None
        self.last_method = None    # 上一次交互的method.
        self.recv = None
        self.error = None
        self.data = None
        self._in = None
        self._out = None

        while True:
            try:
                self.count += 1
                (self.recv, _, self.error) = select.select(
                    self.socs, [], self.socs, 2)
                if self.error:
                    logging.error('select.select error : {}'.format(str(self.error)))
                    break
                if self.recv:
                    for self._in in self.recv:
                        if self._in is self.client:
                            self._out = self.real_db
                            source = 'CLIENT'
                        else:
                            self._out = self.client
                            source = 'REAL_DB'

                        self.data = self.__get_buffer(self._in)
                        self.method = self.__get_method(self.data)

                        self.method_map[self.method]()

                        if self.method != 'NONE':
                            self.count = 0

                        self.data = None

                if self.count == time_out_max:
                    break
            except Exception as e:
                logging.error('Exception: [METHOD]:{} [ERROR]:{} [DATA]:{}'.format(method, str(e), self.data))
                break


def start_server(host=HOST, port=PORT, timeout=TIMEOUT, handler=ConnectionHandler, real_dbs=REAL_DBS):
    soc = socket.socket(socket.AF_INET)
    soc.bind((host, port))
    soc.listen(0)
    while 1:
        # 等待客户端接入
        threading.Thread(target=handler, args=soc.accept() + (timeout, real_dbs,)).start()



if __name__ == '__main__':
    start_server()


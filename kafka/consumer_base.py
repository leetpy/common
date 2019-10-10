# -*- coding: utf-8 -*-

import abc
import logging
import zlib

from confluent_kafka import Consumer


class ConsumerBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, topics, group_id):
        self.consumer = None
        self.servers = '192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092'
        self.topics = topics
        self.group_id = group_id
        self.reconnect()

    def reconnect(self):
        self.consumer = Consumer({
            'bootstrap.servers': self.servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(self.topics)

    def run(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                logging.error("Consumer error:%s" % msg.error())
                continue

            msg_topic = msg.topic()
            msg_content = msg.value()

            try:
                msg_content = zlib.decompress(msg_content)
            except Exception:
                msg_content = msg_content

            msg_value = msg_content.decode('utf-8')
            self.handler_one_message(msg_topic, msg_value)

    @abc.abstractmethod
    def handler_one_message(self, topic, msg):
        print("Base handler_one_message")

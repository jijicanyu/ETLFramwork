"""
Hive Config
"""
import random

from NConfParser import NConfParser


class HiveConfig(object):
    """
    HiveConfig
    """

    def __init__(self):
        """
        ctor
        """
        parser = NConfParser()
        qe_percentage = parser.get('HiveEnv', 'qe_percentage', 'INT')

        self.__engine = "queryengine" \
            if random.randint(0, 100) < qe_percentage \
            else "hive"

        self.__qe_namespace = parser.get('HiveEnv', 'qe_namespace', 'string')
        self.__qe_command = parser.get('HiveEnv', 'qe_command', 'string')
        self.__hive_command = 'hive'

    def get_engine(self):
        """
        :return: engine
        """
        return self.__engine

    def get_command(self):
        """
        :return: command
        """
        if self.__engine == 'queryengine':
            return self.__qe_command + ' '
        else:
            return self.__hive_command + ' '

    def get_namespace(self):
        """
        :return: namespace
        """
        if self.__engine == 'queryengine':
            return 'use namespace ' + self.__qe_namespace + '; '
        else:
            return ''

    def set_engine(self, command):
        """
        :param command:
        :return: None
        """
        if command is None or command == 'queryengine':
            self.__engine = 'queryengine'
        else:
            self.__engine = command


hiveConfig = HiveConfig()

import logging
import logging.config
import os
from NConfParser import NConfParser


np = NConfParser()
logging.config.fileConfig(np.get("log_conf","log_conf_file"))
#create logger
NLogger = logging.getLogger("nquery")
"""
class NLogger:
    np = NConfParser()
    logging.config.fileConfig(np.get("log_conf","log_conf_file"))
    #create logger
    logger = logging.getLogger("example")
    @classmethod
    def info(cls, info):
        NLogger.logger.info(info)   
    @classmethod
    def debug(cls, info):
        NLogger.logger.debug(info)
    @classmethod
    def critical(cls, info):
        NLogger.logger.critical(info)
    @classmethod
    def error(cls, info):
        NLogger.logger.error(info)
    @classmethod
    def warn(cls, info):
        NLogger.logger.warn(info)
"""
if __name__=="__main__":
    a=NLogger
    a.info("bb")
    a.debug("bb")
    a.critical("bb")
    a.error("bb")
    a.warn("bb")
    a.info("bb")

import ConfigParser
import string,os,sys
class  NConfParser:
    def __init__(self, conffile="NQuery.conf"):
        self.cf = ConfigParser.ConfigParser()
        self.cf.read((os.path.dirname(__file__) or ".")+ "/"+conffile)
    
    def get(self, key, item, item_type=""):
        if item_type=="BOOLEAN":
            if self.cf.has_option(key,item):
                return self.cf.getboolean(key, item)
            return True
        elif item_type =="FLOAT":
            if self.cf.has_option(key, item):
                return self.cf.getfloat(key, item)
            return 0.0
        elif item_type=="INT":
            if self.cf.has_option(key, item):
                return self.cf.getint(key, item)
            return 0
        else:
            return self.cf.get(key, item)

    

    

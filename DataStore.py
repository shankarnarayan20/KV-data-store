__author__ = 'Shankar Narayan S'
__email__ = 'shankarnarayan20@gmail.com'

import json
from datetime import datetime,timedelta
import os
import pickle
import fcntl
import threading

class DataStore:
    def __init__(self,path = './data/data.json'):
        self.path = path
        self._lock = threading.Lock()


    def readfromfile(self):
        with self._lock:
            with open(self.path) as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX) 
                if os.path.getsize(self.path) > 0:
                    data = json.load(f)  
                else:
                    data = {}
                f.close()     
        return data          
    
    def writetofile(self,data):
        with self._lock:
            with open(self.path, "w") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX) 
                json.dump(data, f)
                f.close()    
               
    def create(self,key,jsondata):
        
        if len(key) == 0 or not jsondata:
            raise AssertionError

        if os.path.exists(self.path):
            if os.stat(self.path).st_size/1073741824 > 1:
                raise MemoryError('file size greater than 1GB')
        else:
            f = open(self.path,"x")
            f.close()  
        
        writedata = dict()
        writedata[key] = jsondata
        now = datetime.now()
        nowasstring = now.strftime("%d/%m/%Y %H:%M:%S")

        if len(key)>32:
            raise TypeError('length greater than 32')
        
        if len(pickle.dumps(jsondata)) > 16*1024:
            raise MemoryError('Json size must be less than 16KB')  
    
        writedata[key]['createdat'] = nowasstring
        if 'timetolive' in jsondata.keys():
            try:
                int(jsondata['timetolive'])

            except:
                raise TypeError('timetolive is not an integer')   
        
        data = self.readfromfile()        
        if key in data:
            raise KeyError('key is already present')
        
        data.update(writedata)
        self.writetofile(data)
        return "key has been successfully added"

    def read(self,readkey): 

        if os.stat(self.path).st_size/1073741824 > 1:
            raise SystemError('size greater than 1GB')

        data = self.readfromfile()    
        
        if readkey in data:
            if 'timetolive' in data[readkey]:
                datetime_str = data[readkey]['createdat']
                datetime_object = datetime.strptime(datetime_str, '%d/%m/%Y %H:%M:%S')
                timetoliveinsecs = int(data[readkey]['timetolive'])
                if (datetime_object + timedelta(0,timetoliveinsecs)) > datetime.now():
                    value = data[readkey]
                    return "the key value pair is {}, {} ".format(readkey,value)
                else:
                    del data[readkey]
                    self.writetofile(data)
                    raise KeyError('Key timed out')       
            else:
                value = data[readkey]
                return "the key value pair is {}, {} ".format(readkey,value)
                 
        else:
            raise KeyError('key doesnt exist')
            

    def delete(self,deletekey):

        data = self.readfromfile()
        
        if deletekey in data:
            if 'timetolive' not in data[deletekey]:
                del data[deletekey]
                self.writetofile(data)
                return "key deleted"
            else:
                datetime_str = data[deletekey]['createdat']
                datetime_object = datetime.strptime(datetime_str, '%d/%m/%Y %H:%M:%S')
                timetoliveinsecs = int(data[deletekey]['timetolive'])
                if (datetime_object + timedelta(0,timetoliveinsecs)) > datetime.now():
                    del data[deletekey]
                    self.writetofile(data)
                    return "key deleted"
                else:
                    del data[deletekey]
                    self.writetofile(data)
                    raise KeyError ("key timed out")    
        else:    
            raise KeyError ("key doesnt exist")
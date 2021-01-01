import sys
import json
from datetime import datetime,timedelta
import os
import pickle

class DataStore:
    def __init__(self):
        if len(sys.argv) == 1:
            self.path = './data/data.json'
        else: 
            self.path = str(sys.argv[1])
    
    def create(self,key,jsondata):
        
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
                pass
            except:
                raise TypeError('timetolive is not an integer')   
        
        if os.stat(self.path).st_size/1073741824 > 1:
            raise MemoryError('file size greater than 1GB')

        with open(self.path) as f:
            data = json.load(f)
            if key in data:
                raise KeyError('key is already present')
        
        data.update(writedata)
        with open(self.path, "w") as f:
            json.dump(data, f)
        return "key has been successfully added"

    def read(self,readkey): 

        if os.stat(self.path).st_size/1073741824 > 1:
            raise SystemError('size greater than 1GB')

        with open(self.path) as f:
            data = json.load(f)    
        
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
                    with open(self.path, "w") as f:
                        json.dump(data, f)
                    raise KeyError('Key timed out')       
            else:
                value = data[readkey]
                return "the key value pair is {}, {} ".format(readkey,value)
                 
        else:
            raise KeyError('key doesnt exist')
            

    def delete(self,deletekey):

        with open(self.path) as f:
            data = json.load(f)
        
        if deletekey in data:
            if 'timetolive' not in data[deletekey]:
                del data[deletekey]
                with open(self.path, "w") as f:
                    json.dump(data, f)
                return "key deleted"
            else:
                datetime_str = data[deletekey]['createdat']
                datetime_object = datetime.strptime(datetime_str, '%d/%m/%Y %H:%M:%S')
                timetoliveinsecs = int(data[deletekey]['timetolive'])
                if (datetime_object + timedelta(0,timetoliveinsecs)) > datetime.now():
                    del data[deletekey]
                    with open(self.path, "w") as f:
                        json.dump(data, f)
                    return "key deleted"
                else:
                    del data[deletekey]
                    with open(self.path, "w") as f:
                        json.dump(data, f)
                    raise KeyError ("key timed out")    
        else:    
            raise KeyError ("key doesnt exist")
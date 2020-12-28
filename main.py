import flask
import json
from filepath import filepath
from datetime import datetime,timedelta
import os

path = filepath()
app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "Key Value Data Store by Shankar Narayan",200

@app.route('/create', methods=['POST'])
def create():
    req_data = flask.request.get_json()
    now = datetime.now()
    nowasstring = now.strftime("%d/%m/%Y %H:%M:%S")
    
    for req_datakeys in req_data:
        if len(req_datakeys)>32:
            return "length error",400
        req_data[req_datakeys]['createdat'] = nowasstring
        if 'timetolive' in req_data[req_datakeys].keys():
            try:
                int(req_data[req_datakeys]['timetolive'])
                pass
            except:
                return "timetolive is not an integer",400    
    
    if os.stat(path).st_size/1073741824 > 1:
        return "file size greater than 1GB",400.

    with open('./data/data.json') as f:
        data = json.load(f)
    
    for key in req_data:
        if key in data:
            return "key already present",400
     
    data.update(req_data)
    with open('./data/data.json', "w") as f:
        json.dump(data, f)
    return "data has been successfully added",200    

@app.route('/read', methods=['GET'])
def read():
    readkey = flask.request.args.get('key')
    if not readkey:
        return "no key entered", 400    

    if os.stat(path).st_size/1073741824 > 1:
        return "file size greater than 1GB",500

    with open(path) as f:
        data = json.load(f)    

    if readkey in data:
        datetime_str = data[readkey]['createdat']
        datetime_object = datetime.strptime(datetime_str, '%d/%m/%Y %H:%M:%S')
        timetoliveinsecs = int(data[readkey]['timetolive'])
        if (datetime_object + timedelta(0,timetoliveinsecs)) > datetime.now():
            value = data[readkey]
            return "the key value pair is {}, {} ".format(readkey,value),200
        else:
            del data[readkey]
            with open('./data/data.json', "w") as f:
                json.dump(data, f)
            return "key timed out",400        
    else: 
        return "key doesnt exist",400

@app.route('/delete', methods=['DELETE'])
def delete():
    deletekey = flask.request.args.get('key')
    if not deletekey:
        return "no key entered", 400
    with open('./data/data.json') as f:
        data = json.load(f)

    if deletekey not in data:
        return "key doesnt exist",400
    else:
        datetime_str = data[deletekey]['createdat']
        datetime_object = datetime.strptime(datetime_str, '%d/%m/%Y %H:%M:%S')
        timetoliveinsecs = int(data[deletekey]['timetolive'])
        if (datetime_object + timedelta(0,timetoliveinsecs)) > datetime.now():
            del data[deletekey]
            with open('./data/data.json', "w") as f:
                json.dump(data, f)
            return "key deleted",400
        else:
            del data[deletekey]
            with open('./data/data.json', "w") as f:
                json.dump(data, f)
            return "key timed out",400  


    del data[deletekey]

    with open('./data/data.json', "w") as f:
        json.dump(data, f)
    return "deleted successfully",200    

@app.route('/text', methods=['GET'])
def test():
      print(os.stat(path).st_size) 
      return "works",200


app.run(debug=True)
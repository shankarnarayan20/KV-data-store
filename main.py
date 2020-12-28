import flask
import json
from filepath import filepath
from datetime import datetime

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
        req_data[req_datakeys]['CreatedAt'] = nowasstring
        if 'timetolive' in req_data[req_datakeys].keys():
            try:
                int(req_data[req_datakeys]['timetolive'])
                pass
            except:
                return "timetolive is not an integer",400    
    
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
    with open(path) as f:
        data = json.load(f)
    if readkey in data:
        value = data[readkey]
        return "the key value pair is {}, {} ".format(readkey,value),200    
    else: 
        return "key doesnt exist",400

@app.route('/delete', methods=['DELETE'])
def delete():
    deletekey = flask.request.args.get('key')
    deletekeyfunction(deletekey)
    if not deletekey:
        return "no key entered", 400
    with open('./data/data.json') as f:
        data = json.load(f)

    if deletekey not in data:
        return "bad request",400   

    del data[deletekey]
    with open('./data/data.json', "w") as f:
        json.dump(data, f)
    return "deleted successfully",200    


app.run(debug=True)
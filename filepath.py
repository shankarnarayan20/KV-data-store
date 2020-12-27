import sys

def filepath():
    if len(sys.argv) == 1:
        path = './data/data.json'
    else: 
        path = str(sys.argv[1])
    return path

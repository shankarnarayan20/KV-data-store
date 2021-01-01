# Key Value data store with CRD operations

- Basic Create, Read, Delete operation according to the given conditions

# Run tests

```
python3 test.py 
```

# How to use

```python
from DataStore import DataStore
path = './data/newdata.json'

ds = DataStore(path) # location is optional

response = ds.create('k', {'k1': 'v1'})
print(response)

value = ds.read('k')
print(value)

response = ds.delete('k')
print(response)

# With time to live
response = ds.create('k', {'k1': 'v1','timetolive':60})
print(response)
```

- Unit tested in Ubuntu 20.04 
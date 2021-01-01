from main import DataStore



d = DataStore()
# msg = d.create("123456789",{"value":"sadhash","timetolive":15})
# print(msg)
ans = d.delete("123456789")
print(ans)
# ans = d.delete("withtimetoliweve1234")
# print(ans)


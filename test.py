from DataStore import DataStore
import threading
import unittest
import time

location = './data/testing.json'

with open ('./data/testing.json',"w") as f:
    f.write('')
class test(unittest.TestCase):
    def setUp(self):
        self.ds = DataStore(location)

    def test_create(self):
        self.ds.create('k',{'k1':'v1'})
        self.assertIsNotNone(self.ds.read('k'))

    def test_with_repeated_key(self):
        self.ds.create('k',{'k1':'v12'})
        with self.assertRaises(KeyError):
            self.ds.create('k',{'k1':'v12'})

    def test_with_wrong_key(self):
        self.ds.create('k2',{'k1':'v12'})
        with self.assertRaises(KeyError):
            self.ds.read('k123')
            
    def test_create_with_ttl_expired(self):
        self.ds.create('k1',{'k1':'v1','timetolive':2})
        time.sleep(3)
        with self.assertRaises(KeyError):
            self.ds.read('k1')
    
    def test_with_key_null(self):
        with self.assertRaises(AssertionError):
            self.ds.create('',{'k1':'v1'})

    def test_with_value_null(self):
        with self.assertRaises(AssertionError):
            self.ds.create('k',{})        
    
    def test_with_long_key(self):
        with self.assertRaises(TypeError):
            self.ds.create('ajbdshjasbdhasjbdhasjlbdhasjdbhasdasdasdasd',{'k1':'v1'})

    def test_with_invalid_ttl_type(self):
        with self.assertRaises(TypeError):
            self.ds.create('k',{'k1':'v1','timetolive':'string'})          

    def test_with_timedout_key_del(self):
        self.ds.create('k1',{'k1':'v1','timetolive':2})
        time.sleep(3)
        with self.assertRaises(KeyError):
            self.ds.delete('k1')  

    def test_with_wrong_key_del(self):
        self.ds.create('k1',{'k1':'v12'})
        with self.assertRaises(KeyError):
            self.ds.delete('k123')        

    def test_del(self):
        self.assertIsNotNone(self.ds.delete('k'))

    def test_threading(self):
        newthread = threading.Thread(target = self.ds.create,args = ('kthread1',{'k1':'v1'},))
        newthread2 = threading.Thread(target = self.ds.create,args = ('kthread2',{'k1':'v1'},))
        newthread.start()
        newthread2.start()
        newthread.join()
        newthread2.join()
        self.assertIsNotNone(self.ds.read('kthread1'))

if __name__ == '__main__':
    unittest.main()        



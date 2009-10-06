import random
import logging

from google.appengine.ext import db
from google.appengine.api import memcache

class Counter(db.Model):
    name = db.StringProperty(required = True)
    count = db.IntegerProperty(required = True, default = 0)

def get_count(name):
    total = memcache.get(name)
    if total is None:
        total = 0
        for counter in Counter.gql("WHERE name = :1", name):
            total += counter.count
        memcache.add(name, str(total), 60)
    return total
        

def increment(name):
    def tnx():
        index = random.randint(0, 5)
        shard_name = name + str(index)
        counter = Counter.get_by_key_name(shard_name)
        if counter is None:
            counter = Counter(key_name=shard_name, name=name)
        counter.count += 1
        counter.put()
        
    db.run_in_transaction(tnx)
    memcache.incr(name)

def decrement(name):
    def tnx():
        index = random.randint(0, 5)
        shard_name = name + str(index)
        counter = Counter.get_by_key_name(shard_name)
        if counter is None:
            counter = Counter(key_name=shard_name, name=name)
        counter.count -= 1
        counter.put()
        
    db.run_in_transaction(tnx)    
    memcache.decr(name)    
    logging.info("Descrementing %s %s" % (name, memcache.get(name)))
from random import random, randrange
from uuid import uuid4
from time import time

from kafka_backed_dict import KafkaBackedDict


kafka_brokers = 'put_your_kafka_brokers_here:9092'

# open up key-value store and clear it out
a = KafkaBackedDict(kafka_brokers,
                    'test.spencerrules.1',
                    db_dir='/home/vagrant')
#                    prefix_extractor_transform=lambda key: (0,1))  # test the cool prefix feature later... for now just ignore this
for k in a.keys():
    del(a[k])

# open up an ordinary dict to compare against
b = {}


## time how long it takes to add/update a million records
#starttime = time()
#print(starttime)
#for i in range(0, 1000000):
#    k = randrange(0, 10000)
#    if k in a:
#        count = a[k]
#    else:
#        count = 0
#    a[k] = count
#endtime = time()
#
#print("total time = %f" % (endtime - starttime))


# now try a bunch of random adds, changes, and deletes to both KafkaBackedDict and regular python dict
for i in range(10000):
    # half the time add a new key (randomly generated)
    if random() < 0.5:
        k = str(uuid4()).encode()
        v = str(uuid4()).encode()
        a[k] = v
        b[k] = v
        #print("added %s=%s" % (k,v))
    else:
        # make sure key lists are the same
        keys_a = sorted(list(a.keys()))
        keys_b = sorted(list(b.keys()))
        assert keys_a == keys_b

        # pick a random key
        if not keys_a:
            continue
        k = keys_a[randrange(0, len(keys_a))]

        # quarter of the time mutate an existing key 
        if random() < 0.5:
            v = str(uuid4()).encode()
            a[k] = v
            b[k] = v
            #print("modified %s=%s" % (k,v))

        # quarter of the time delete an existing key
        else:
            del(a[k])
            del(b[k])
            #print("deleted %s" % (k))

# now validate that they are the same
print("validating")
keys_a = sorted(list(a.keys()))
keys_b = sorted(list(b.keys()))
assert keys_a == keys_b

for k in keys_b:
    assert a[k] == b[k]

# start a new database instance and revalidate
print("revalidating")
del a
a = KafkaBackedDict('internal-PRD1KAFKA-1272792231.us-west-2.elb.amazonaws.com:9092', 'test.spencerrules.1', db_dir='/home/vagrant')
keys_a = sorted(list(a.keys()))
keys_b = sorted(list(b.keys()))
assert keys_a == keys_b

for k in keys_b:
    assert a[k] == b[k]

print("Congrats, everything passed")

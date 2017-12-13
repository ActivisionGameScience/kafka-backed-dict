from ._kafka_client import KafkaClient
from . import _settings

import os
import ujson
from base64 import b64encode, b64decode
import rocksdb

import time
from uuid import uuid4
import logging


logger = logging.getLogger(__name__)
logger.setLevel(_settings.LOG_LEVEL)
logger.addHandler(logging.StreamHandler())


class PrefixExtractor(rocksdb.interfaces.SliceTransform):
    def name(self):
        return b'prefix_extractor'

    def transform(self, key):
        raise NotImplementedError("must specify a prefix_extractor function")

    def in_domain(self, key):
        return True

    def in_range(self, prefix):
        return True


class KafkaBackedDict(object):

    def __init__(self, 
                 kafka_bootstrap_servers,
                 kafka_topic,
                 partition=None,
                 use_rocksdb=True,
                 db_dir=None,
                 rocksdb_mem=4194304,
                 catchup_delay_seconds=30,
                 guid=None,
                 prefix_extractor_transform=None):
        self._kafka_bootstrap_servers = kafka_bootstrap_servers
        self._kafka_topic = kafka_topic
        if partition:
            raise NotImplementedError("multiple partitions not supported yet")
        self._use_rocksdb = use_rocksdb
        self._db_dir = db_dir
        self._catchup_delay_seconds = catchup_delay_seconds

        # if not specified then use cwd
        if not self._db_dir:
            self._db_dir = os.getcwd()

        # this guid will be used everywhere
        self.guid = guid
        if not guid:
            self.guid = str(uuid4())

        # open kafka
        self._kafka = KafkaClient(self._kafka_bootstrap_servers,
                                 self._kafka_topic,
                                 self.guid)

        # open db (either ordinary dict or rocksdb)
        if self._use_rocksdb:
            self._db_path = os.path.join(self._db_dir, "rocksdb-" + self.guid)
            rocksdb_options = rocksdb.Options(create_if_missing=True,
                                              write_buffer_size=rocksdb_mem/2,
                                              max_write_buffer_number=2)
            if prefix_extractor_transform:
                self._prefix_extractor = PrefixExtractor()
                self._prefix_extractor.transform = prefix_extractor_transform
                rocksdb_options.prefix_extractor = self._prefix_extractor
            else:
                self._prefix_extractor = None
            self._db = rocksdb.DB(self._db_path, rocksdb_options)
        else:
            self._db = {}

        # we will read from kafka periodically, but haven't read yet
        self._last_catchup = 0

    def __getitem__(self, key):
        return self.get(key)[0]  # drop timestamp_ms

    def get(self, key):
        if not isinstance(key, bytes):
            key = str(key).encode()

        self._catchup()

        if self._use_rocksdb:
            val = self._db.get(key)
            if not val:
                raise KeyError(key)
            return self._decode_val(val)
        else:
            val = self._db[key]
            return self._decode_val(val)

    def _decode_val(self, val):
        val = ujson.loads(val.decode('utf-8'))
        # has format [timestamp_ms=int, base64_encoded=0 or 1, data]
        if val[1] == 1:
            return b64decode(val[2]), val[0]
        else:
            return val[2], val[0]

    def __setitem__(self, key, val):
        self.set(key, val)

    def set(self, key, val, timestamp_ms=None):
        if not isinstance(key, bytes):
            key = str(key).encode()
        val = self._encode_val(val, timestamp_ms)

        # produce to kafka
        self._kafka.produce(key, val)

        # write locally
        if self._use_rocksdb:
            self._db.put(key, val)
        else:
            self._db[key] = val

    def _encode_val(self, val, timestamp_ms):
        if not timestamp_ms:
            timestamp_ms = int(time.time()*1000)
        if isinstance(val, bytes):
            val = [timestamp_ms, 1, b64encode(val)]
        else:
            val = [timestamp_ms, 0, val]
        val = ujson.dumps(val, ensure_ascii=False).encode('utf-8')
        return val

    def __delitem__(self, key):
        if not isinstance(key, bytes):
            key = str(key).encode()

        # produce tombstone to kafka
        self._kafka.produce(key, b'__delete_key__')

        # delete locally
        if self._use_rocksdb:
            self._db.delete(key)
        else:
            del(self._db[key])

    def keys(self):
        self._catchup()

        if self._use_rocksdb:
            it = self._db.iterkeys()
            it.seek_to_first()

            return it
        else:
            return self._db.keys()

    def values(self):
        self._catchup()

        if self._use_rocksdb:
            it = self._db.itervalues()
            it.seek_to_first()

            for val in it:
                yield self._decode_val(val)[0]
        else:
            for val in self._db.values():
                yield self._decode_val(val)[0]

    def items(self, prefix=None):
        self._catchup()

        if not prefix:
            if self._use_rocksdb:
                it = self._db.iteritems()
                it.seek_to_first()

                for k, v in it:
                    yield k, self._decode_val(v)[0]
            else:
                for k, v in self._db.values():
                    yield k, self._decode_val(v)[0]
        else:
            if not self._prefix_extractor:
                raise RuntimeError("prefix search only supported if you pass a prefix_extractor_transform function in the constructor")
            if not isinstance(prefix, bytes):
                prefix = str(prefix).encode()
                         
            if self._use_rocksdb:
                it = self._db.iteritems()
                it.seek(prefix)

                for k, v in it:
                    start, end = self._prefix_extractor.transform(k)
                    if prefix == k[start:end]:
                        yield k, self._decode_val(v)[0]
                    else:
                        break
            else:
                raise NotImplementedError("prefix search only supported if using rocksdb")

    def first_item(self):
        self._catchup()

        if not self._use_rocksdb:
            raise NotImplementedError("last_item only supported if using rocksdb")
        it = self._db.iteritems()
        it.seek_to_first()

        for item in it:
            return item[0], self._decode_val(item[1])[0]

    def last_item(self):
        self._catchup()

        if not self._use_rocksdb:
            raise NotImplementedError("last_item only supported if using rocksdb")
        it = self._db.iteritems()
        it.seek_to_last()

        for item in reversed(it):
            return item[0], self._decode_val(item[1])[0]

    def __iter__(self):
        self._catchup()

        for k in self.keys():
            yield k

    def _catchup(self):
        if time.time() - self._last_catchup < self._catchup_delay_seconds:
            return
        self._last_catchup = time.time()
        self._kafka.flush_producer()
        for key, val, ts_millis in self._kafka.consume():
            #print("Received %s=%s at ts=%d" % (key, val, ts_millis))
            if self._use_rocksdb:
                if val != b'__delete_key__':
                    self._db.put(key, val)
                else:  # received tombstone
                    self._db.delete(key)
            else:
                if val != b'__delete_key__':
                    self._db[key] = val
                else:
                    del(self._db[key])

# Dict-like python object that persists to Kafka (optionally caches to RocksDB)

**This was POC only.  There is a much better project
called [Faust](https://github.com/robinhood/faust) (that is, if you can work with Python's `async`).**

This python 3 library provides a persistent dict-like data structure.  Key-value
pairs are persisted in Kafka, and optionally can be cached locally in RocksDB.  For
smaller datasets it is possible to cache in memory by setting `use_rocksdb=False` (see below).

Multiple clients can share the same topic, but there are no consistency guarantees - 
last write wins.

## Quickstart for Conda users in Linux

Using the `conda` package manager is the quickest way to get going
without building anything:
```
# bootstrap the conda system
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
/bin/bash Miniconda3-latest-Linux-x86_64.sh

# point to our conda channel
echo "channels:\n  - ActivisionGameScience\n  - defaults" > ~/.condarc

# create and activate an environment
conda create -n fooenv python=3.6 kafka-backed-dict ipython -y
source activate fooenv

# start ipython and you're cooking!
```

## Usage

The API is similar to that of a dict:
```
    from kafka_backed_dict import KafkaBackedDict

    # `db_dir` is where the RocksDB database will be stored
    # (NOTE: RocksDB doesn't play nice with Windows filesystem)
    # if you only want in-mem then pass `use_rocksdb=False` instead
    d = KafkaBackedDict('my.kafkabootstrapserver.com:9092', 'my.kafkatopic.1', db_dir='/tmp', read_only=False)

    # keys are encoded into byte strings (so 'key1' becomes b'key1', and 5 becomes b'5')
    # values go through a round-trip json conversion - if you don't like this then you
    # can always store raw bytes yourself, e.g. b'24321'
    d['key1'] = 234
    print(d['key1'])  # prints 234

    d['key2'] = b'24321'
    print(d['key2'])  # prints b'24321'

    d['key3'] = {'foo': 'bar', 1: 5}
    print(d['key3'])  # prints {'foo': 'bar', '1': 5}... notice that 1 was changed to '1' by the json converter

    # you can delete keys easily
    del(d['key1'])
    d['key1']  # raises KeyError

    # you can iterate over all keys
    for k in d.keys():
        print(k)
        print(d[k])
```

## Build

You can build and install manually with the following command:
```
    VERSION="0.1.0" python setup.py install
```
where `0.1.0` should be replaced with whatever tag you checked out.

A conda build recipe is also provided (currently only works in Linux).  Assuming you have your
environment set up (see e.g. https://github.com/ActivisionGameScience/ags_conda_recipes.git),
you can build the package by running
```
    VERSION="0.1.0" conda build conda_recipe
```

## License

All files are licensed under the BSD 3-Clause License as follows:
 
> Copyright (c) 2016, Activision Publishing, Inc.  
> All rights reserved.
> 
> Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
> 
> 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
>  
> 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
>  
> 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
>  
> THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


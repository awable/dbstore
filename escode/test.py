import escode
import json
import cjson
import bson
import random
import time
import cProfile

input_types = [
    None,
    True,
    False,
    int(-1),
    int(0),
    int(1),
    int((1 << 32) - 1),
    int((1 << 32)),
    int((1 << 31) - 1),
    int((1 << 31)),
    int(-(1 << 31)),
    int(-(1 << 31) - 1),
    long(-1),
    long(0),
    long(1),
    #long((1 << 64) - 1),
    long((1 << 63) - 1),
    #long((1 << 63)),
    long(-(1 << 63)),
    0.0,
    123.123,
    "abcde",
    u"\xe1\xe9\xed\xf3\xfa\xfc\xf1\xbf\xa1",
    [],
    [1,2],
    {},
    ]

inputs = input_types + [input_types]
input = dict((str(random.randrange(0, 1 << 64)), e) for e in input_types)

def do(n, enc, dec):
    s = time.time()
    for i in xrange(10000):
        e = enc(input)
    et = time.time() - s
    s = time.time()
    for i in xrange(10000):
        d = dec(e)
    dt = time.time() -s
    assert d == input
    print n,et,dt

do('cjson', cjson.encode, cjson.decode)
do('json', json.dumps, json.loads)
do('bson', bson.BSON.encode, bson.BSON.decode)
do('escode', escode.encode, escode.decode)

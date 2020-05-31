# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function

__author__ = 'ethan'
MULTI_THREADED = False
# from array import array as pyarray
from cpython cimport tuple, bool, long, list


import itertools
import os.path
import sqlite3
import struct
import time
import sys

def _execute(cursor, *args):
    assert isinstance(cursor, sqlite3.Cursor)
    cursor.execute(*args)
    return cursor

class BetterCursor(sqlite3.Cursor):
    def iter_dicts(self):
        c_columns = tuple(i[0] for i in self.description)
        for row in self:
            yield {k: v for (k, v) in zip(c_columns, row)}

cpdef bool testPrime(list foundPrimes, long candidatePrime):
    cdef long candidatePrimeHint = (candidatePrime >> 1)

    for oldPrime in foundPrimes:
        if candidatePrimeHint < oldPrime:
            # if the candidate is between
            # (prime before oldPrime * 2) and (oldPrime * 2), there is
            # no whole number between 1 and 2 to use.
            return True

        elif 0 == (candidatePrime % oldPrime):
            return False # Not a prime
    else:
        return True

class PrimeDB(object):
    PRIMES_FILE_MAGIC = "$PRIMES_DB$"
    PRIMES_FILE_HEADER = struct.Struct('{}sQ'.format(len(PRIMES_FILE_MAGIC)))

    def __init__(self):
        self.foundPrimes = [] # Array of unsigned longs
        self.foundPrimes.append(2)

    def storeToFile(self, fp="primes.bin"):
        with open(fp, 'wb') as fd:
            primeCount = len(self.foundPrimes)
            print("storing {} primes, the largest being {}".format(primeCount, self.foundPrimes[-1]))
            fd.write(self.PRIMES_FILE_HEADER.pack(self.PRIMES_FILE_MAGIC, primeCount))
            fd.write( struct.pack("{}Q".format(primeCount), *self.foundPrimes))
            # self.foundPrimes.tofile(fd)
            fd.truncate()

    def storeToDb(self, fp="primes.db"):
        conn = sqlite3.connect(fp)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor(BetterCursor)
        cursor.execute('DROP TABLE IF EXISTS PRIMES_T;')
        cursor.execute('''CREATE TABLE PRIMES_T ( PrimeX INTEGER UNIQUE );''')
        paramList = [ (primeX,) for primeX in self.foundPrimes ]
        cursor.executemany( '''INSERT INTO PRIMES_T (PrimeX) VALUES (?)''', paramList )
        conn.commit()
        cursor.close()
        conn.close()

    def loadFromFile(self, fp="primes.bin"):
        if not os.path.isfile(fp):
            self.storeToFile(fp)

        del self.foundPrimes  # throw away the primes list
        self.foundPrimes = list()  # Array of unsigned longs
        with open(fp, 'rb') as fd:
            primeCount = self.PRIMES_FILE_HEADER.unpack(fd.read(self.PRIMES_FILE_HEADER.size))[1]
            print("loading {} primes".format(primeCount))
            valStruct = struct.Struct("{}Q".format(primeCount))
            self.foundPrimes.extend(valStruct.unpack(fd.read(valStruct.size)))
            # self.foundPrimes.fromfile(fd, primeCount)
            print("loaded {} primes, the largest being {}".format(primeCount, self.foundPrimes[-1]))

    def testPrime(self, const long candidatePrime):
        cdef long candidatePrimeHint = (candidatePrime >> 1)

        for oldPrime in self.foundPrimes:
            if candidatePrimeHint < oldPrime:
                # if the candidate is between
                # (prime before oldPrime * 2) and (oldPrime * 2), there is
                # no whole number between 1 and 2 to use.
                return True

            elif 0 == (candidatePrime % oldPrime):
                return False # Not a prime
        else:
            return True

    def resume2(self, maxTryCount=1000000):
        counter = itertools.count((self.foundPrimes[-1] + 1), 1)

        cdef int foundPrimeCount = 0
        cdef float startTime = time.clock()

        for tryCountX in xrange(maxTryCount):
            candidatePrime = counter.next()

            # if candidatePrime % 100000 == 0:
            #     self.storeToFile()

            if self.testPrime(candidatePrime):
                foundPrimeCount += 1
                self.foundPrimes.append(candidatePrime)

        endTime = time.clock()
        foundPrimeRatio = float(foundPrimeCount) / float(maxTryCount)
        print("found {} primes in {} numbers ({:%} primes)".format(foundPrimeCount, maxTryCount, foundPrimeRatio))
        dur = (endTime - startTime)
        durPerLoop = (dur/maxTryCount)
        durPerPrime = (dur/foundPrimeCount)
        print("took {}s ({}s/loop, {}s/prime)".format(dur, durPerLoop, durPerPrime))

    def resume3(self, const int maxTryCount=1000):
        cdef int CandidateStart = (self.foundPrimes[-1] + 1)
        cdef int CandidateEnd = (maxTryCount + CandidateStart + 1)
        cdef tuple CandidateRange = tuple( range( CandidateStart, CandidateEnd ) )
        # cdef tempFoundPrimes = self.foundPrimes

        cdef int foundPrimeCount = 0
        cdef float startTime = time.clock()

        for candidatePrime in CandidateRange:
            # if candidatePrime % 100000 == 0:
            #     self.storeToFile()

            if testPrime(self.foundPrimes, candidatePrime):
                foundPrimeCount += 1
                # tempFoundPrimes.extend_buffer(candidatePrime)
                self.foundPrimes.append(candidatePrime)


        # self.foundPrimes = [ prime for prime in tempFoundPrimes ]
        cdef float endTime = time.clock()
        cdef float foundPrimeRatio = float(foundPrimeCount) / float(maxTryCount)
        print("found {} primes in {} numbers ({:%} primes)".format(foundPrimeCount, maxTryCount, foundPrimeRatio))
        dur = (endTime - startTime)
        durPerLoop = (dur/maxTryCount)
        durPerPrime = (dur/foundPrimeCount)
        print("took {}s ({}s/loop, {}s/prime)".format(dur, durPerLoop, durPerPrime))



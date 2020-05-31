# coding: utf-
from __future__ import print_function
import array
import collections
import itertools
import os.path
import sqlite3
import struct
import threading
from multiprocessing import Pool
import time
import math

__author__ = 'ethan'


def _execute(cursor, *args):
    assert isinstance(cursor, sqlite3.Cursor)
    cursor.execute(*args)
    return cursor


class BetterCursor(sqlite3.Cursor):
    def iter_dicts(self):
        c_columns = tuple(i[0] for i in self.description)
        for row in self:
            yield {k: v for (k, v) in zip(c_columns, row)}


def staticTestPrime(foundPrimes, candidatePrime):
    candidatePrimeHint = (candidatePrime >> 1)

    for oldPrime in foundPrimes:
        if candidatePrimeHint < oldPrime:
            # if the candidate is between
            # (prime before oldPrime * 2) and (oldPrime * 2), there is
            # no whole number between 1 and 2 to use.
            return True

        elif 0 == (candidatePrime % oldPrime):
            return False  # Not a prime
    else:
        return True


class PrimeDB(object):
    PRIMES_FILE_MAGIC = "$PRIMES_DB$"
    PRIMES_FILE_MAGIC2 = "$PRIME_DB2$"
    PRIMES_FILE_HEADER = struct.Struct('{}sQ'.format(len(PRIMES_FILE_MAGIC)))

    def __init__(self):
        self.foundPrimes = array.array('L')  # Array of unsigned longs
        self.foundPrimes.append(2L)
        # self.candidatePrimes = collections.deque()
        # self.foundPrimesBuffer = collections.deque()
        # self._foundPrimesBufferLock = threading.RLock()

    def storeToFile(self, fp="primes.bin"):
        with open(fp, 'wb') as fd:
            primeCount = len(self.foundPrimes)
            print("storing {} primes, the largest being {}".format(primeCount, self.foundPrimes[-1]))
            fd.write(self.PRIMES_FILE_HEADER.pack(self.PRIMES_FILE_MAGIC, primeCount))
            self.foundPrimes.tofile(fd)
            fd.truncate()

    def storeToFileLarge(self, fp="primes.bin"):
        with open(fp, 'wb') as fd:
            primeCount = len(self.foundPrimes)
            print("storing {0} primes, the largest being {1} (0x{1:016X})".format(primeCount, self.foundPrimes[-1]))
            fd.write(self.PRIMES_FILE_HEADER.pack(self.PRIMES_FILE_MAGIC, primeCount))
            tempPrimes = array.array('L')
            tempPrimes.extend(self.foundPrimes.tolist())
            tempPrimes.tofile(fd)
            fd.truncate()

    def storeToFileSmall(self, fp="primes2.bin"):
        with open(fp, 'wb') as fd:
            primeCount = len(self.foundPrimes)
            formatFullness = self.foundPrimes[-1] / float(0xFFFFFFFF)
            print("storing {0} primes, the largest being {1} (0x{1:08X}) ({2:%} max for db format)".format(
                primeCount, self.foundPrimes[-1], formatFullness))
            fd.write(self.PRIMES_FILE_HEADER.pack(self.PRIMES_FILE_MAGIC2, primeCount))
            tempPrimes = array.array('I')
            tempPrimes.extend(self.foundPrimes.tolist())
            tempPrimes.tofile(fd)
            fd.truncate()

    def storeToDb(self, fp="primes.db"):
        conn = sqlite3.connect(fp)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor(BetterCursor)
        cursor.execute('''DROP TABLE IF EXISTS PRIMES_T;''')
        cursor.execute('''CREATE TABLE PRIMES_T ( PrimeX INTEGER UNIQUE );''')
        paramList = [(primeX,) for primeX in self.foundPrimes.tolist()]
        cursor.executemany('''INSERT INTO PRIMES_T (PrimeX) VALUES (?)''', paramList)
        conn.commit()
        cursor.close()
        conn.close()

    def iterFrequency(self):
        return [(i, x, (float(i)/x),math.sqrt(float(i)/x)) for i, x in enumerate(self.foundPrimes, start=1)]

    def loadFromFile(self, fp="primes2.bin"):
        if not os.path.isfile(fp):
            self.storeToFileSmall(fp)

        del self.foundPrimes  # throw away the primes list

        with open(fp, 'rb') as fd:
            magic, primeCount = self.PRIMES_FILE_HEADER.unpack(fd.read(self.PRIMES_FILE_HEADER.size))
            if magic == self.PRIMES_FILE_MAGIC:
                self.foundPrimes = array.array('L')  # Array of unsigned longs
            elif magic == self.PRIMES_FILE_MAGIC2:
                self.foundPrimes = array.array('I')  # Array of unsigned ints
            print("loading {} primes".format(primeCount))
            self.foundPrimes.fromfile(fd, primeCount)
            print("loaded {} primes, the largest being {}".format(primeCount, self.foundPrimes[-1]))

    def testableSlice(self):
        """
        :rtype: slice
        """
        latestPrime = self.foundPrimes[-1]
        return slice(latestPrime + 1, ((latestPrime * 2) - 1), 1)

    # def flush(self):
    #     with self._foundPrimesBufferLock:
    #         self.foundPrimes.extend( sorted(self.foundPrimesBuffer) )
    #         self.foundPrimesBuffer.clear()

    def testPrime(self, candidatePrime):
        candidatePrimeHint = (candidatePrime >> 1)

        for oldPrime in self.foundPrimes:
            if candidatePrimeHint < oldPrime:
                # if the candidate is between
                # (prime before oldPrime * 2) and (oldPrime * 2), there is
                # no whole number between 1 and 2 to use.
                return True

            elif 0 == (candidatePrime % oldPrime):
                return False  # Not a prime
        else:
            return True

    def resume(self, maxTryCount=10000):
        counter = itertools.count((self.foundPrimes[-1] + 1), 1)

        foundPrimeCount = 0
        startTime = time.clock()

        for tryCountX in xrange(maxTryCount):
            candidatePrime = counter.next()

            if candidatePrime % 100000 == 0:
                self.storeToFile()

            candidatePrimeHint = (candidatePrime >> 1)

            foundPrime = False
            for oldPrime in self.foundPrimes:
                if candidatePrimeHint < oldPrime:
                    # if the candidate is between
                    # (prime before oldPrime * 2) and (oldPrime * 2), there is
                    # no whole number between 1 and 2 to use.
                    foundPrime = True
                    # print "early out for {} against {}".format(candidatePrime, oldPrime)
                    break
                elif 0 == (candidatePrime % oldPrime):
                    break  # Not a prime
            else:
                # print "late out for {} against {}".format(candidatePrime, oldPrime)
                foundPrime = True
            if foundPrime:
                foundPrimeCount += 1
                self.foundPrimes.append(candidatePrime)
                # timeOfNewPrime = time.time()
                # print candidatePrime, len(foundPrimes)
                # print candidatePrime, "took", timeOfNewPrime- timeOfLastPrime
                # timeOfLastPrime = timeOfNewPrime

        endTime = time.clock()
        foundPrimeRatio = float(foundPrimeCount) / float(maxTryCount)
        print("found {} primes in {} numbers ({:%} primes)".format(foundPrimeCount, maxTryCount, foundPrimeRatio))
        dur = (endTime - startTime)
        durPerLoop = (dur/maxTryCount)
        durPerPrime = (dur/foundPrimeCount)
        print("took {}s ({}s/loop, {}s/prime)".format(dur, durPerLoop, durPerPrime))

    def resume2(self, maxTryCount=1000000):
        ProcessCount = 1
        counter = itertools.count((self.foundPrimes[-1] + 1), 1)

        foundPrimeCount = 0
        startTime = time.clock()

        _counter_next = counter.next  # optimize instructions
        _self_test_prime = self.testPrime  # optimize instructions

        for tryCountX in xrange(maxTryCount):
            candidatePrime = _counter_next()

            # if candidatePrime % 100000 == 0:
            #     self.storeToFile()

            if _self_test_prime(candidatePrime):
                foundPrimeCount += 1
                self.foundPrimes.append(candidatePrime)

        endTime = time.clock()
        foundPrimeRatio = float(foundPrimeCount) / float(maxTryCount)
        print("found {} primes in {} numbers ({:%} primes)".format(foundPrimeCount, maxTryCount, foundPrimeRatio))
        dur = (endTime - startTime)
        durPerLoop = (dur/maxTryCount)
        durPerPrime = (dur/foundPrimeCount)
        print("took {}s ({}s/loop, {}s/prime)".format(dur, durPerLoop, durPerPrime))
        print("took {}s total compute time".format(dur * ProcessCount))

    def resume3(self, maxTryCount=1000000, ProcessCount=3):
        StartOfCandidatePrimeRange = (self.foundPrimes[-1] + 1)
        EndOfCandidatePrimeRange = (StartOfCandidatePrimeRange + maxTryCount)
        # initTime = time.time()
        pool = Pool(processes=ProcessCount)
        foundPrimeCount = 0
        # print "took an additional {}s to open the pool".format(time.time() - initTime)
        startTime = time.clock()

        lastProcessedCandidate = 0
        while lastProcessedCandidate < EndOfCandidatePrimeRange:
            s = self.testableSlice()
            xStart = s.start
            xStep = s.step
            xStop = min((s.stop, EndOfCandidatePrimeRange))
            # print("Processing {}->{}".format(xStart, xStop))
            inputs, allCurrentTestableCandidates = itertools.tee(xrange(xStart, xStop, xStep), 2)
            outputs = pool.map(self.testPrime, inputs)
            newFoundPrimes = list(itertools.compress(allCurrentTestableCandidates, outputs))
            foundPrimeCount += len(newFoundPrimes)
            self.foundPrimes.extend(newFoundPrimes)
            lastProcessedCandidate = xStop
            print("Processed {}->{}".format(xStart, xStop))

        endTime = time.clock()
        foundPrimeRatio = float(foundPrimeCount) / float(maxTryCount)
        print("found {} primes in {} numbers ({:%} primes)".format(foundPrimeCount, maxTryCount, foundPrimeRatio))
        dur = (endTime - startTime)
        durPerLoop = (dur/maxTryCount)
        durPerPrime = (dur/foundPrimeCount)
        print("took {}s ({}s/loop, {}s/prime)".format(dur, durPerLoop, durPerPrime))
        print("took {}s total compute time".format(dur * ProcessCount))
        pool.close()
        pool.join()


def main(CandidateCount, PoolCount, DBFileName="primes2.bin"):
    primeDb = PrimeDB()
    primeDb.loadFromFile(DBFileName)

    if PoolCount < 0:
        raise ValueError("PoolCount must be greater than or equal to 0!")

    print("\n----------\nUsing {} pools to test {} candidates:".format(PoolCount, CandidateCount))

    if PoolCount == 0:
        primeDb.resume2(CandidateCount)
    else:
        primeDb.resume3(CandidateCount, PoolCount)

    primeDb.storeToFileSmall()


if __name__ == '__main__':
    # main(10000, 0)
    main(30000, 3)

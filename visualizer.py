# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function

__author__ = 'ethan'

import matplotlib.pyplot as plt
import pylab

import Primes


primeDb = Primes.PrimeDB()
primeDb.loadFromFile()

act_pct_primes = primeDb.foundPrimes.tolist()
i = range(1, len(act_pct_primes) + 1)
sim_05pct_primes = [(ii / 0.05) for ii in i]
sim_06pct_primes = [(ii / 0.06) for ii in i]
sim_07pct_primes = [(ii / 0.07) for ii in i]


plt.rcParams["figure.figsize"] = (22,17)

plt.plot(sim_05pct_primes, i, color="red", label="5.0% freq.")
plt.plot(sim_06pct_primes, i, color="orange", label="6.0% freq.")
plt.plot(sim_07pct_primes, i, color="green", label="7.0% freq.")
plt.plot(act_pct_primes, i, label="Prime # freq.")
plt.title("Primes")
plt.xlabel("Value")
plt.ylabel("Count")
plt.legend(loc="upper left")
plt.grid(True)
plt.show()


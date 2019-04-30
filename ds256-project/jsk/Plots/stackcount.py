import numpy as np
import matplotlib.pyplot as plt

N = 6
erasure = [1650, 1366, 1045, 762, 444, 0]
replicated = [0, 160, 310, 445, 590, 772]

ind = np.arange(N)  # the x locations for the groups
width = 0.35  # the width of the bars: can also be len(x) sequence

p1 = plt.bar(ind, erasure, width)
p2 = plt.bar(ind, replicated, width,
             bottom=erasure)
plt.xlabel("Storage efficiency")
plt.ylabel("Number of blocks ")

plt.title('RS (6,4) Recovery 2 nodes failure (End to End)')
plt.xticks(ind, ('1.5', '1.8', '2.1', '2.4', '2.7', '3.0'))
plt.legend((p1[0], p2[0]), ('erasure', 'replicated'))

axes = plt.gca()
axes.set_ylim([0, 1750])

plt.show()

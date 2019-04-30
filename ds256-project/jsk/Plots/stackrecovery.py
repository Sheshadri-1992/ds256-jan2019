import numpy as np
import matplotlib.pyplot as plt

N = 6
erasure = [155.5, 139.3, 94.3, 79.7, 48.68, 0]
replicated = [0, 7.71, 16.8, 27.16, 33.3, 48.201]

ind = np.arange(N)  # the x locations for the groups
width = 0.35  # the width of the bars: can also be len(x) sequence

p1 = plt.bar(ind, erasure, width)
p2 = plt.bar(ind, replicated, width,
             bottom=erasure)
plt.xlabel("Storage efficiency")
plt.ylabel("Time (in secs)")

plt.title('RS(6,4) Recovery 2 nodes failure (End to End)')
plt.xticks(ind, ('1.5', '1.8', '2.1', '2.4', '2.7', '3.0'))
plt.legend((p1[0], p2[0]), ('erasure', 'replicated'))

axes = plt.gca()
axes.set_ylim([0,160])

plt.show()

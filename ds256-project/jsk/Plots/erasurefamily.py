import matplotlib.pyplot as plt
import numpy as np

X = ["1.5", "1.35", "1.25"]
Y = [43.549, 48, 45]
Z = [175, 315, 376]

fig, ax = plt.subplots()

ind = np.arange(3)  # the x locations for the groups
width = 0.35  # the width of the bars
p1 = ax.bar(ind, Y, width, color='deepskyblue', bottom=0)

p2 = ax.bar(ind + width, Z, width,
            color='tomato', bottom=0)

ax.set_xticks(ind + width / 2)
plt.xticks(np.arange(3),('1.5', '1.35', '1.25'))

plt.legend((p1[0], p2[0]), ('put', 'get'))

plt.xlabel("Storage efficiency")
plt.ylabel("Time (in secs)")
plt.title("110 put/get req of 10MB ~ 1GB (End to End)")

plt.show()

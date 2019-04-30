import matplotlib.pyplot as plt

X = ["1.5", "1.8", "2.1", "2.4", "2.7", "3"]
Y = [175, 163, 127, 98, 76, 61]

plt.xlabel("Storage efficiency")
plt.ylabel("Time (in secs)")
plt.title("RS(6,4) 110 get req of 10MB ~ 1GB (End to End)")

plt.plot(X, Y, marker='o')
plt.show()

import matplotlib.pyplot as plt

X = ["1.5", "1.8", "2.1", "2.4", "2.7", "3"]
Y = [43.549, 49.794, 55, 60, 64, 70]

plt.xlabel("Storage efficiency")
plt.ylabel("Time (in secs)")
plt.title("RS(6,4) 110 put req of 10MB ~ 1GB (End to End)")

plt.plot(X,Y,marker='o')
plt.show()
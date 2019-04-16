import matplotlib.pyplot as plt

X = [1,2,3,4,5,6,7,8]
Y = [42362,127087,211812,254174,296536,338898,423623,465985]

plt.plot(X,Y,marker='o',color='g')
plt.title("Distinct Approximate Counts")
plt.xlabel('Time (in mins)')
axes = plt.gca()
axes.set_ylim([42362,475985])
plt.ylabel('Number of distinct approximate users')
plt.show()
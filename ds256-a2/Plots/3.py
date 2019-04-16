import matplotlib.pyplot as plt

X = [3,6,9,12,15,18,21,24,27]
Y = [55000,66300,77500,83000,83200,83500,68500,60000,80500]

plt.plot(X,Y,marker='o')
plt.title("Sentiment Analysis")
plt.xlabel('Time (in mins)')
axes = plt.gca()
axes.set_xlim([0,30])
plt.ylabel('Number of tweets processed')
plt.show()

# total = 0
# for x in Y:
# 	total = total + x

# result = float(total/(27*60.0))
# print result

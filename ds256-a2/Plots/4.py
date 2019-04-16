import matplotlib.pyplot as plt

# X = [3,6,9,12,15,18,21,24,27]
# Y1 = [36500,58000,70150]
# Y2 = [23500,40000,43500,50000,20500]
# Y3 = [25000,26000,26000,28700,30000,30800,1800]

X = ["1","2","4"]
Y = [91.47,109.56,133.57]

plt.bar(X,Y)

plt.title("Weak Scaling")
plt.xlabel('Delay introduced (in ms)')
# axes = plt.gca()
# axes.set_xlim([0,30])
plt.ylabel('Num tweets processed / executor')
plt.show()

# total = 0
# for x in Y1:
# 	total = total + x

# result = float(total/(7.5*4*60.0))
# print result, result*4

# total = 0
# for x in Y2:
# 	total = total + x

# result = float(total/(13.5*2*60.0))
# print result, result*2

# total = 0
# for x in Y3:
# 	total = total + x

# result = float(total/(21*1*60.0))
# print result
import matplotlib.pyplot as plt

X = [5,10,15,20,25,30,35]#[0,5,[
Y = [107925,136043,142145,121340,115123,125988,126585] #[0,30849,

# plt.plot(X,Y,marker='o')
# plt.title("Data pre-processing")
# plt.xlabel('Time (in mins)')
# plt.ylabel('Number of tweets processed')
# plt.show()

total = 0
for x in Y:
	total = total + x

result = float(total/(35*60.0))
print result

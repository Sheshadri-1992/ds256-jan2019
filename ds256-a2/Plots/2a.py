import matplotlib.pyplot as plt

X = [10,15,20,25,30,35,40] #[0,5,[
Y = [107925,136043,142145,121340,115123,125988,126585] #[0,30849,

plt.plot(X,Y,marker='o',color='g')
plt.title("Data pre-processing")
plt.xlabel('Time (in mins)')
plt.ylabel('Number of tweets processed')
plt.show()
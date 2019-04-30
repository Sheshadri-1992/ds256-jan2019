import matplotlib.pyplot as plt

era_put_file = "/home/swamiji/eclipse-workspace/jsk/1.5/elogs_put.txt"
rep_put_file = "/home/swamiji/eclipse-workspace/jsk/1.5/rlogs_put.txt"
era_get_file = "/home/swamiji/eclipse-workspace/jsk/1.5/elogs_get.txt"
rep_get_file = "/home/swamiji/eclipse-workspace/jsk/1.5/rlogs_get.txt"

X = ["1.5", "1.8", "2.1", "2.4", "2.7", "3"]
# Y = [ ]

with open(era_put_file) as f:
    era_put = f.readlines()

with open(rep_put_file) as f:
    rep_put = f.readlines()

with open(era_get_file) as f:
    era_get = f.readlines()

with open(rep_get_file) as f:
    rep_get = f.readlines()

era_put_time = 0
for ele in era_put:
    era_put_time = era_put_time + int(ele.split(",")[2])

era_get_time = 0
for ele in era_get:
    era_get_time = era_get_time + int(ele.split(",")[2])

rep_put_time = 0
for ele in rep_put:
    rep_put_time = rep_put_time + int(ele.split(",")[2])

rep_get_time = 0
for ele in rep_get:
    rep_get_time = rep_get_time + int(ele.split(",")[2])

print("ERA_PUT_TIME : ", era_put_time, " REP_PUT_TIME ", rep_put_time)
print("ERA_GET_TIME : ", era_get_time, " REP_GET_TIME ", rep_get_time)

print("Total put time ", (era_put_time + rep_put_time))
print("Total get time ", (era_get_time + rep_get_time))
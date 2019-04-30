import matplotlib.pyplot as plt

era_blocks = "/home/swamiji/eclipse-workspace/jsk/3.0_recovery/elogs_recovery.txt"
rep_blocks = "/home/swamiji/eclipse-workspace/jsk/3.0_recovery/rlogs_recovery.txt"

with open(era_blocks) as f:
    era_put = f.readlines()

with open(rep_blocks) as f:
    rep_put = f.readlines()

era_time = 0
for ele in era_put:
    era_time = era_time + int(ele.split(",")[2])

rep_time = 0
for ele in rep_put:
    rep_time = rep_time + int(ele.split(",")[2])

print("era time ", era_time, " rep_time is ", rep_time)

import os
import sys

fs = ["ja.txt", "en.txt", "zh.txt", "ko.txt", "fr.txt", "de.txt", "sp.txt"]
l = []
for i, f in enumerate(fs):
    with open(f, "r") as ff:
        l.append(ff.readlines())

with open("merge.txt", "w") as f:
    for i in range(len(l[0])):
        line = ''
        for j in range(len(l)):
            line += l[j][i].strip() + '\t'
        f.write(line.strip()+'\n')
            

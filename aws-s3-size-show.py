from os import walk
# DEFAULT VARS
dir_log = "out/"
logfile_prefix = "aws-s3-size-"
denom = 40


f = []
for (dirpath, dirnames, filenames) in walk(dir_log):
    f.extend(filenames)
    break
ss={}
for i in range(len(f)):
    s  = f[i]
    s  = s.replace(".log","").replace(logfile_prefix,"")
    ss[s] = {}
    with open(dir_log+f[i], 'r', encoding='utf-8') as ff:
        for ii in ff:
            line = (ii).strip().split(";")
            ss[s][line[0]] = dict(size=line[2],count=line[3])

# prepare dict
workgroups = {i:{} for i in ss[next(iter(ss))]}

for i in ss:
    for ii in ss[i]:
        workgroups[ii][i]=ss[i][ii]["size"]

wrkgrp_zero = []
wrkgrp_not_exists = []
wrkgrp_to_render = []
for i in workgroups:
    # prewatching with zero and exists
    is_zero=True
    is_not_exists=True
    for j in workgroups[i]:
        if int(workgroups[i][j])>0:
            is_zero = False
        if int(workgroups[i][j])>-1:
            is_not_exists = False
    if is_not_exists:
        wrkgrp_not_exists.append(i)
    else:
        if is_zero:
            wrkgrp_zero.append(i)
        else:
            wrkgrp_to_render.append(i)
        
colors = {
    "head":'\033[4m',
    "info":'\033[0m',
    "warning":'\033[93m',
    "error":'\033[91m',
    "ok":'\033[92m'
}
sizes = {
    0:"b",
    1:"Kb",
    2:"Mb",
    3:"Gb",
    4:"Tb"
}

print(colors["info"]+"Workgroup locations not exists:")
for i in wrkgrp_not_exists:
    print(colors["info"]+"  ", colors["error"]+i)
print()

print(colors["info"]+"Workgroup locations with zero size:")
for i in wrkgrp_zero:
    print(colors["info"]+"  ", colors["warning"]+i)
print()
# print(workgroups)

print(colors["info"]+"Workgroup locations:")
for i in wrkgrp_to_render:
    print(colors["info"]+"  ", i)
    # work with bar
    ww = [int(workgroups[i][a]) for a in workgroups[i]]
    min_v = min(ww)
    max_v = max(ww)
    part = (max_v + min_v)//2//(denom//2)
    # print(part)
    prev = 0
    for ii in workgroups[i]:
        val = int(workgroups[i][ii])
        if prev == 0:
            prev = val
        x,y = val,0

        ye,mo,da = ii[0:4],ii[4:6],ii[6:8]
        ho,mi,se = ii[9:11],ii[11:13],ii[13:15]
        ss = da+"."+mo+"."+ye+" "+ho+":"+mi+":"+se
        while x>=1024:
            x //= 1024
            y +=  1
        nom = val//part
        bar = "="*nom
        l = (colors["info"]+"    "+ss+"("+str(x)+sizes[y]+")").ljust(35)
        l += bar.ljust(denom+4)
        if prev == val:
            l += colors["info"]+str(val)
        if prev > val:
            l += colors["warning"]+str(val)
        if prev < val:
            l += colors["ok"]+str(val)
        l+= colors["info"]+" bytes"

        print(l)
        prev = val

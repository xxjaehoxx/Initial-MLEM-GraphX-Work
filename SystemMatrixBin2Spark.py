import csv
import time
import os
import struct
import binascii

timebefore=time.time()
f = open("/global/project/projectdirs/paralleldb/sw/nerscpower/jae_NERSC/360_views.sm", "rb")

Nsinogram = struct.unpack('I', f.read(4))[0]
Nvolume = struct.unpack('I', f.read(4))[0]

with open('SystemMatrix.csv', 'wb') as csvfile:
    myFile = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for ns in range(0,Nsinogram):
        N = struct.unpack('I', f.read(4))[0]
        Nchunk = struct.unpack('I', f.read(4))[0]
        Itemp = [];
        SMtemp = [];
        for n in range(0,Nchunk):
            Itemp.append(struct.unpack('I', f.read(4))[0])
        for n in range(0,Nchunk):
            SMtemp.append(struct.unpack('<f', f.read(4))[0])
        for n in range(0,Nchunk):
            myFile.writerow([N] + [Itemp[n]] + [SMtemp[n]])
       
f.close()
exectime=time.time()-timebefore
print "Time required to convert the binary file to a readable file: %s seconds"%(exectime)
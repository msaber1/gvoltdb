#!/usr/bin/python

# This file is part of VoltDB.
# Copyright (C) 2008-2014 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys
import math

def process_line(line, num):
    array = [0] * num
    l = line.split()
    label = int(l[0])
    data = l[1:]

    for p_d in data:
        p, d = p_d.split(':')
        array[int(p) - 1] = float(d)

    return label, array

def process_file(filename, num):
    f = open(filename)
    labels = []
    data = []
    for line in f:
        if line.strip():
            label, d = process_line(line.strip(), num)
            labels.append(label)
            data.append(d)

    f.close()
    return labels, data

def logsigmoid(weights, label, data):
    s = 0.0
    for w,d in zip(weights, data):
        s += w*d

    return -math.log((1+math.exp(-label*s)))

def calculate_lost(weights, labels, data):
    lost = 0.0
    for l,d in zip(labels, data):
        lost += -logsigmoid(weights, l, d)

    return lost

def calculate_all(weightfile, labels, data):
    f = open(weightfile)
    losts = []
    for line in f:
        if line.strip():
            weights = line.split()
            weights = map(float, weights)
            lost = calculate_lost(weights, labels, data)
            print lost
            losts.append(lost)
    f.close()
    return losts

def main(datafile, weightfile, num):
    print 'load data'
    labels, data = process_file(datafile, num)
    calculate_all(weightfile, labels, data)

if __name__ == "__main__":
    datafile = sys.argv[1]
    weightfile = sys.argv[2]
    num = int(sys.argv[3])
    main(datafile, weightfile, num)

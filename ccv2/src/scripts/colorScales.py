#!/usr/bin/env python
# Colors from http://geography.uoregon.edu/datagraphics/color_scales.htm

import math
import sys

def int2hex(i):
    if i<16:
        return "0" + '%X'%i
    else:
        return '%X'%i

# Based on five steps sequential, but removing the 5th from each (to light)
four_steps_sequential_rgb256 = [
    "153      15      15",
    "178      44      44",
    "204      81      81",
    "229     126     126",
#    "255     178     178",
    "153      84      15",
    "178     111      44",
    "204     142      81",
    "229     177     126",
#    "255     216     178",
    "107     153      15",
    "133     178      44",
    "163     204      81",
    "195     229     126",
#    "229     255     178",
    "15     107     153",
    "44     133     178",
    "81     163     204",
    "126     195     229",
#    "178     229     255",
    "38      15     153",
    "66      44     178",
    "101      81     204",
    "143     126     229",
#    "191     178     255"
]
def four_steps_sequential():
    colors = []
    for c in four_steps_sequential_rgb256:
        rgb256 = c.split()
        colors.append("#" + int2hex(int(rgb256[0])) + int2hex(int(rgb256[1])) + int2hex(int(rgb256[2])))
    return colors

green_to_magenta_rgb256 =[
"0      80       0",
"0     134       0",
"0     187       0",
"0     241       0",
"80     255      80",
"134     255     134",
"187     255     187",
#"255     255     255",  # Don't use WHITE
"255     241     255",
"255     187     255",
"255     134     255",
"255      80     255",
"241       0     241",
"187       0     187",
"134       0     134",
"80       0      80"]
def green_to_magenta():
    colors = []
    for c in green_to_magenta_rgb256:
        rgb256 = c.split()
        colors.append("#" + int2hex(int(rgb256[0])) + int2hex(int(rgb256[1])) + int2hex(int(rgb256[2])))
    return colors
        
blue_to_dark_orange_rgb256 = [
"0     102     102",
"0     153     153",
"0     204     204",
"0     255     255",
"51     255     255",
"101     255     255",
"153     255     255",
"178     255     255",
"203     255     255",
"229     255     255",
"255     229     203",
"255     202     153",
"255     173     101",
"255     142      51",
"255     110       0",
"204      85       0",
"153      61       0",
"102      39       0",
]
def blue_to_dark_orange():
    colors = []
    for c in blue_to_dark_orange_rgb256:
        rgb256 = c.split()
        colors.append("#" + int2hex(int(rgb256[0])) + int2hex(int(rgb256[1])) + int2hex(int(rgb256[2])))
    return colors
    
def matlab_jet(count):
    colors = []
    n = math.ceil(count/4.0)

    iter = int(n)
    # u vector
    u = []
    for i in range(0,iter):
        u.append((i+1)/n)
    for i in range(1,iter):
        u.append(1)
    for i in range(iter,0,-1):
        u.append(i/n)

    ur = u[:]
    ur.reverse()

    # rgb vectors
    r = []
    g = []
    b = []
    t1 = math.ceil(n/2.0)
    t2 = 0
    if ((count % 4) == 1):
        t2 = 1
    for i in range(0,len(u)):
        val = t1 - t2 + i +1
        g.append(val)
        r.append(val + n)
        b.append(val - n)


    # remove elements
    l = g[:]
    for e in l:
        if (e > count):
            g.remove(e)
    l = r[:]
    for e in l:
        if (e > count):
            r.remove(e)
    l = b[:]
    for e in l:
        if (e < 1):
            b.remove(e)

    ub = u[:];
    ub.reverse();
    ub = ub[0:len(b)];
    ub.reverse();

    # create colormap
    for i in range(1, count+1):

        red = 0;
        blue = 0;
        green = 0;
        
        if i in r:
            ri = r.index(i)
            red = int(255*u[ri])

        if i in g:
            gi = g.index(i)
            green = int(255*u[gi])

        if i in b:
            bi = b.index(i)
            blue = int(255*ub[bi]);
        
        st = "#" + int2hex(red) + int2hex(green) + int2hex(blue)
        colors.append(st)
        
    return colors
        
    
        
    

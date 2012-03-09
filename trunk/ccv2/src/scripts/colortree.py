#!/usr/bin/python
# $Id$

import sys
import os
import math

__version__ = "$Revision$"

#
# Color utilities and classes
#

def decoratePhyloXMLFile(inPath, strainColorer, idToNames = None, outPath = None):
    """
    decorates a ccv generated PhyloXML file
    inPath: a filename
    strainColorer: a colorer, which has a single method, color(), which takes the
    strainID. See ColoringFastaTable below.
    idToNames: an optional mapping from the keys in idToClusters to
        replacement names for the XML nodes
    outPath: a filename (optional)
    """
    import xml.dom.minidom
    tree = xml.dom.minidom.parse(inPath)

    names = tree.getElementsByTagName("name")
    for n in names:
        name = ""
        for child in n.childNodes:
            if child.nodeType == child.TEXT_NODE:
                name += child.data
        # If the name we've collected is one of the
        # leaves in our visualization, then replace it with
        # a readable name, and color it, and rebuild the node.
        color = strainColorer.color(name)
        if color is not None:
            n.setAttribute("color", color)
            if idToNames is not None and idToNames.has_key(name):
                newName = idToNames[name]
                while n.firstChild:
                    n.removeChild(n.firstChild)
                n.appendChild(tree.createTextNode(newName))
    s = tree.toxml()
    if outPath is not None:
        fp = open(outPath, "w")
        fp.write(s)
        fp.close()
    return s

# Some colors are from http://geography.uoregon.edu/datagraphics/color_scales.htma
# Based on five steps sequential, but removing the 5th from each (too light)

def int2hex(i):
    """Used to turn ints into hex for web-colors"""
    if i<16:
        return "0" + '%X'%i
    else:
        return '%X'%i
        
four_steps_sequential_rgb256 = [
    (153,      15,      15),
    (178,      44,      44),
    (204,      81,      81),
    (229,     126,     126),
#    (255,     178,     178),
    (153,      84,      15),
    (178,     111,      44),
    (204,     142,      81),
    (229,     177,     126),
#    (255,     216,     178),
    (107,     153,      15),
    (133,     178,      44),
    (163,     204,      81),
    (195,     229,     126),
#    (229,     255,     178),
    (15,     107,     153),
    (44,     133,     178),
    (81,     163,     204),
    (126,     195,     229),
#    (178,     229,     255),
    (38,      15,     153),
    (66,      44,     178),
    (101,      81,     204),
    (143,     126,     229),
#    (191,     178,     255)
    ]
    
def four_steps_sequential():
    colors = []
    for c in four_steps_sequential_rgb256:
        rgb256 = c.split()
        colors.append("#" + int2hex(int(rgb256[0])) + int2hex(int(rgb256[1])) + int2hex(int(rgb256[2])))
    return colors
    
green_to_magenta_rgb256 =[
    (0,      80,       0),
    (0,     134,       0),
    (0,     187,       0),
    (0,     241,       0),
    (80,     255,      80),
    (134,     255,     134),
    (187,     255,     187),
    # (255,     255,     255),  # Don't use WHITE
    (255,     241,     255),
    (255,     187,     255),
    (255,     134,     255),
    (255,      80,     255),
    (241,       0,     241),
    (187,       0,     187),
    (134,       0,     134),
    (80,       0,      80)
    ]
     
def green_to_magenta():
    colors = []
    for c in green_to_magenta_rgb256:
        rgb256 = c.split()
        colors.append("#" + int2hex(int(rgb256[0])) + int2hex(int(rgb256[1])) + int2hex(int(rgb256[2])))
    return colors

blue_to_dark_orange_rgb256 = [
    (0,     102,     102),
    (0,     153,     153),
    (0,     204,     204),
    (0,     255,     255),
    (51,     255,     255),
    (101,     255,     255),
    (153,     255,     255),
    (178,     255,     255),
    (203,     255,     255),
    (229,     255,     255),
    (255,     229,     203),
    (255,     202,     153),
    (255,     173,     101),
    (255,     142,      51),
    (255,     110,       0),
    (204,      85,       0),
    (153,      61,       0),
    (102,      39,       0)
    ]

def blue_to_dark_orange():
    colors = []
    for c in blue_to_dark_orange_rgb256:
        rgb256 = c.split()
        colors.append("#" + int2hex(int(rgb256[0])) + int2hex(int(rgb256[1])) + int2hex(int(rgb256[2])))
    return colors

# The original colorer.

class OrderColorer:

    def __init__(self, colors = None):
        self.colors = []
        if colors is not None:
            self.colors = colors

    def color(self, clusterNum):
        return self.colors[clusterNum % len(self.colors)]

    # Any colorer can be cached.
    
    def store(self, file, numClusters):
        fp = open(file, "w")
        for i in range(numClusters):
            fp.write("%d %s\n" % (i, self.color(i)))
        fp.close()

class CachedFileColorer:

    def __init__(self, file):
        self.colorTable = {}
        fp = open(file, "r")
        for line in fp.readlines():
            toks = line.strip().split(" ", 1)
            if len(toks) == 2:
                [clustNum, color] = toks
                self.colorTable[int(clustNum)] = color
        fp.close()

    def color(self, clusterNum):
        return self.colorTable[clusterNum]            

class SimpleOrderColorer(OrderColorer):

    def __init__(self):
        OrderColorer.__init__(self, 
                              # Set up color array (need to edit later)
                              ['#0000FF', '#DC143C', '#008000', '#FFFF00',
                               '#FFD700', '#00FF00',
                               '#FF1493','#9932CC', '#BDB76B', '#FFA500',
                               '#00FFFF', '#FF4500', 
                               '#B8860B', '#C0C0C0', '#000000' ])

class TupleColorer(OrderColorer):

    def __init__(self, tuples):
        # RGB tuples to hex
        OrderColorer.__init__(self, ["#%02x%02x%02x" % t for t in tuples])

class FourStepsSequentialColorer(TupleColorer):

    def __init__(self):
        TupleColorer.__init__(self, four_steps_sequential_rgb256)

class GreenToMagentaColorer(TupleColorer):

    def __init__(self):
        TupleColorer.__init__(self, green_to_magenta_rgb256)

class BlueToDarkOrangeColorer(TupleColorer):

    def __init__(self):
        TupleColorer.__init__(self, blue_to_dark_orange_rgb256)

class JetColorer(TupleColorer):
    """Similar to matlab_jet colorer"""
    def __init__(self, count):

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
                red = int(255*u[ri]*.8)

            if i in g:
                gi = g.index(i)
                green = int(255*u[gi]*.8)

            if i in b:
                bi = b.index(i)
                blue = int(255*ub[bi]);


            colors.append((red, green, blue))

        TupleColorer.__init__(self, colors)


# And here's an object which encapsulates the strain coloring.

class StrainColorer:

    def __init__(self, ap_path, colorer):
        self.colorer = colorer                    
        #
        # Read an ap output file from ccv. The result will be a pair of
        # dictionaries, one from ID to cluster integer, and
        # another from cluster integer to IDs.
        # 
        self.keyDict = {}
        self.clusterDict = {}
        keys = []
        
        fp = open(ap_path, "r")
        for line in fp.readlines():
            # Use the name in the ap file
            toks = line.strip().split("\t")
            [key, cluster] = toks
            keys.append(key)
            self.keyDict[key] = cluster
            if self.clusterDict.has_key(cluster):
                self.clusterDict[cluster].append(key)
            else:
                self.clusterDict[cluster] = [key]
        fp.close()

    def setColorer(self, colorer):
        self.colorer = colorer

    def color(self, strainID, default = None):
        try:
            return self.colorer.color(int(self.keyDict[strainID]))
        except KeyError:
            return default

    def numClusters(self):
        return len(self.clusterDict.keys())
        
    def numStrains(self):
        return len(self.keyDict.keys())
        
    def clusterOrder(self):
        return self.clusterDict.keys()
#
# MAIN
#
if __name__ == "__main__":
    if not len(sys.argv) >= 3:
        print "Usage: " + sys.argv[0] + " <xml tree file> <ap file> [colorer]";
        print "Options:"
        print "   colorer(int) 1:JetColorer(default), 2:SimpleOrderColorer (15 colors),"
        print "                3:FourStepsSequentialColorer,"
        print "                4:GreenToMagentaColorer (15 colors), 5:BlueToDarkOrangeColorer (18 colors)"
        sys.exit(2)
    
    # Read clusters
    strainColorer = StrainColorer(sys.argv[2], None)
    
    numClusters = strainColorer.numClusters()
    
    if (len(sys.argv) >= 4):
        ct = sys.argv[3]
        if (ct == "1"):
            colorer = JetColorer(numClusters)
            print >>sys.stderr, "Using JetColorer"
        if (ct == "2"):
            colorer = SimpleOrderColorer()
            print >>sys.stderr, "Using SimpleOrderColorer"
        if (ct == "3"):
            colorer = FourStepsSequentialColorer()
            print >>sys.stderr, "Using FourStepsSequentialColorer"
        if (ct == "4"):
            colorer = GreenToMagentaColorer()
            print >>sys.stderr, "Using GreenToMagentaColorer"
        if (ct == "5"):
            colorer = BlueToDarkOrangeColorer()
            print >>sys.stderr, "Using BlueToDarkOrangeColorer"
    else:
        colorer = JetColorer(numClusters)
    
    strainColorer.setColorer(colorer)
    
    #sys.stderr.write( str(len(colorer.colors)) )
    print >>sys.stderr, "Found %d clusters and %d strains" % (numClusters, strainColorer.numStrains())
    print decoratePhyloXMLFile(sys.argv[1], strainColorer)

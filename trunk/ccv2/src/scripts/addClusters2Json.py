# $Id: addClusters2Json.py 986 2008-08-20 17:42:33Z mcolosimo $
"""
addClusters2Json

Adds clusters from the CCV clusters file to a json vectors file.

@author: Marc E. Colosimo, created 1 July 2008
@copyright: The MITRE Corporation, ALL RIGHTS RESERVED, 2008
@version: $Revision: 986 $
"""

import cjson
import sys

def read_clusters(file_name):
    """
    Returns a unique listing of the clusters 
    and the mapping of names to cluster id.

    The cluster file format is as follows:
    sample_name<tab>cluster_id

    @return: tuple with cluster id list and sample names to cluster id
    """
    cmap={}
    imap={}
    handle = open(file_name, "rU")
    for line in handle:
        all = line.strip().split('\t')
        cmap[all[1]] = all[1]
        imap[all[0]] = all[1]
    
    clist = cmap.keys()
    handle.close()
    
    return (clist, imap)

def add_clusters(json_file, clusterTuple):
    handle = open(json_file, "rU")
    json = cjson.decode(handle.read())
    
    # add clusters 
    clusterList = clusterTuple[0]
    clusterList.sort()
    json["clusters"] = clusterList
    
    # add clusters to samples
    samples = json["samples"]  # fixed was  sample
    imap = clusterTuple[1]
    for sample in samples:
        if imap.has_key(sample["name"]):
            sample["cluster"] = imap[sample["name"]]
        # should error check
        
    # dump it out again
    print cjson.encode(json)

if __name__ == "__main__":
    add_clusters(sys.argv[2], read_clusters(sys.argv[1]))
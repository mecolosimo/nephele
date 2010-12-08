/**
 * DistributionIndex.java
 *
 * Created on May 4, 2008, 1:38:01 PM
 *
 * $Id$
 */
package org.mitre.ccv.index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class that holds the complete composition (distribution of n-grams/k-mers)
 * for a set of sequences.
 *
 * @author Marc Colosimo
 */
public class DistributionIndex {

    private static final Log LOG = LogFactory.getLog(DistributionIndex.class);
    
    /** 
     * Mapping of a string to a Map of SequenceNodes using their SeqId.
     */
    private Map<String, Map<Integer, SequenceNode>> nmerMap;
    
    /**
     * Construct a new <tt>DistributionIndex</tt> object.
     */
    public DistributionIndex() {
        this.nmerMap = new HashMap<String, Map<Integer, SequenceNode>>();
    }
    
    /**
     * Adds the str (tile) to the current index and returns the index.
     * 
     * @param seqId the sequence to increase the str count
     * @param str the string (tile) to increase the count
     * @param the count to increase by
     * @return the new count for this seqId and str.
     */
    public Integer addCount(Integer seqId, String str, Integer count) {
        Map<Integer, SequenceNode> sm = this.nmerMap.get(str);
        if (sm == null) {
            sm = new HashMap<Integer, SequenceNode>();
            this.nmerMap.put(str, sm);
            
            /** Create a SequenceNode and store the count */
            SequenceNode sn = new SequenceNode(seqId);
            sn.count = count;
            sm.put(seqId, sn);

            return count;
        } 
        if (sm.containsKey(seqId)) {
            SequenceNode sn = sm.get(seqId);
            sn.count += count;
            
            return sn.count;
        } else {
            /** Create a SequenceNode and store the count */
            SequenceNode sn = new SequenceNode(seqId);
            sn.count = count;
            sm.put(seqId, sn);

            return count;
        }
    }
    
    /**
     * Return the count for the given string and sequence.
     * 
     * @param str the tile (sub-string)
     */
    public Integer getCount(Integer seqId, String str) {
        Map<Integer, SequenceNode> sm = this.nmerMap.get(str);
        if ( sm == null )
            return 0;
        else {
            SequenceNode sn = sm.get(seqId);
            if ( sn == null ) {
                return 0;
            } else {
                return sn.count;
            }
        }
    }

    /**
     * Sums the counts for the given string across all sequences.
     *
     * @param str the n-mer
     * @return the sum, returns zero if none found.
     */
    public Integer sumCounts(String str) {
        Map<Integer, SequenceNode> sm = this.nmerMap.get(str);
        if ( sm == null )
            return 0;
        else {
            Integer count = 0;
            for(Integer seqId : sm.keySet()) {
                count += sm.get(seqId).count;
            }
            return count;
        }
    }
    
    public void setPiValueForNmer(Integer seqId, String nmer, Double pi) {
        SequenceNode sn = this.nmerMap.get(nmer).get(seqId);
        sn.piValue = pi;
    }
    
    /**
     * Gets the pi-value for the given n-mer and sequence.
     *
     * @param seqId the sequence to look up.
     * @param nmer the n-mer to retrieve the pi-value for.
     * @return The pi-value for the given n-mer (this can return <code>null</code>)
     */
    public Double getPiValueForNmer(Integer seqId, String nmer) {
        Map<Integer, SequenceNode> sm = this.nmerMap.get(nmer);
        if ( sm != null ) {
            SequenceNode sn = sm.get(seqId);
            if ( sn != null )
                return sn.piValue;
        }
        return null;
    }
    
    /**
     * Return the number of strings stored
     */
    public Integer size() {
        return this.nmerMap.size();
    }
    
    /**
     * Return the set of <B>ALL</B> strings stored
     */
    public Set<String> getStrings() {
        return this.nmerMap.keySet();
    }
    
    /**
     * Gets the complete set of n-mers for the given sequence.
     *
     * @param the sequence id.
     * @return the <code>Set</code> of n-mers.
     */
    public Set<String> getSequenceNmers(Integer seqId) {
        HashSet<String> set = new HashSet<String>();
        for(String key : this.nmerMap.keySet() ) {
            Map<Integer, SequenceNode> sm = this.nmerMap.get(key);
            if(sm.containsKey(seqId))
                set.add(key);
        }
        return set;
    }
    
    /**
     * Gets the set of n-mers for the given sequence and window size.
     *
     * @param seqId the sequence id.
     * @param windowSize the window size.
     * @return <code>Set</code> of n-mers.
     */
    public Set<String> getSequenceNmers(Integer seqId, Integer windowSize) {
        HashSet<String> set = new HashSet<String>();
        for(String key : this.nmerMap.keySet() ) {
            if (key.length() != windowSize)
                continue;
            Map<Integer, SequenceNode> sm = this.nmerMap.get(key);
            if(sm.containsKey(seqId))
                set.add(key);
        }
        return set;
    }
    
    /**
     * Class for holding information about a nmer for a sequence, such as
     * the sequence identifier (@see IndexedCompleteCompositionVectorSet),
     * count, and Pi-value.
     */
    private class SequenceNode {
        public Integer seqId;
        public Integer count;
        public Double piValue;
        
        public SequenceNode(Integer seqId) {
            this.seqId = seqId;
            this.count = 0;
            this.piValue = null;                   
        }
    }
}

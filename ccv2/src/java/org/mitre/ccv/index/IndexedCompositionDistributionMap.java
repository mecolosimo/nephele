/**
 * IndexCompositionDistributionMap.java
 *
 * Created on May 4, 2008, 2:27:22 PM
 *
 * $Id$
 */
package org.mitre.ccv.index;

import java.util.Iterator;
import org.mitre.ccv.CompositionDistributionMap;

/**
 *
 * @author Marc Colosimo
 */
public class IndexedCompositionDistributionMap implements CompositionDistributionMap {
    private final DistributionIndex distIndex;
    private final Integer windowSize;
    private final Integer seqId;

    /** The number of n-mers we have added */
    private Integer size;


    public IndexedCompositionDistributionMap(Integer windowSize, 
            DistributionIndex index, Integer seqId ) {
        this.windowSize = windowSize;
        this.distIndex = index;
        this.size = 0;
        this.seqId = seqId;
    }
    
    public IndexedCompositionDistributionMap(Integer windowSize, 
            DistributionIndex index, Integer seqId, String sequence) {
        this.windowSize = windowSize;
        this.distIndex = index;
        this.size = 0;
        this.seqId = seqId;
        this.addSequence(sequence);
    }
    
    public Integer getWindowSize() {
        return this.windowSize;
    }
    
    public Integer getSeqId() {
        return this.seqId;
    }

    /**
     * Add the string (tile), increasing its count.
     * 
     * @param str
     * @return
     */
    public boolean put(String str) {
        Integer count = this.distIndex.addCount(this.seqId, str, 1);
        if (count == 1) {
            this.size++;
        } else {
          //  System.out.printf("windowSize=%d, %s, count=%d\n", windowSize, str, count);
        }
        return true;
    }
    
    /**
     * Return the count of the string (tile).
     */
    public Integer get(String str) {
        return this.distIndex.getCount(this.seqId, str);
    }
    
   
    /**
     * Add the sequence to this distribution map.
     */
    public void addSequence(String inSequence) {
        for (int i = 0; i < inSequence.length() - this.windowSize + 1; ++i) {
            String subst = new String(inSequence.substring(i, i + this.windowSize)); 
            this.put(subst);
        }
    }

    /**
     * Add another composition distribution map to this map.
     *
     * @param map the composition distribution to add.
     * @throws java.lang.IllegalArgumentException
     */
    public void addMap(CompositionDistributionMap map) throws IllegalArgumentException {
        for (Iterator<String> iter = map.iterator(); iter.hasNext();) {
            this.put(iter.next());
        }
    }

    /**
     * Return an iterator for all strings indexed/counted.
     */
    public Iterator<String> iterator() {
       return this.distIndex.getSequenceNmers(this.seqId, this.windowSize).iterator();
    }

    /**
     * Returns the size (number) of strings we have counted.
     */
    public Integer size() {
        return this.size;
    }  
}

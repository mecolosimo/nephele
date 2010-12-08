/**
 * Created 28 Nov 2007
 * 
 * $Id$
 */

package org.mitre.ccv;

import java.util.HashMap;
import java.util.Set;

/**
 * Stores an index for strings (tiles).
 *  
 * @author Marc Colosimo
 */
public class SequenceCompositionDistributionIndex {
    /** Mapping of a string to an index. */
    private HashMap<String, Integer> subMap;
    
    /** Reverse mapping from the index to string. */
    private HashMap<Integer, String> idxMap;
    
    /** Stores the number of this put was called for a string. 
    private HashMap<Integer, Integer> idxCnt;  
    */
    
    /** Current index to use (really next index). */
    private Integer curIndex;
    
    public SequenceCompositionDistributionIndex() {
        
        /** These could be speed up if we give them more initial capacity */
        this.subMap = new HashMap<String, Integer>();
        this.idxMap = new HashMap<Integer, String>();
        //this.idxCnt = new HashMap<Integer, Integer>();
        this.curIndex = 0;
    }
    
    /**
     * Adds the str (tile) to the current index and returns the index.
     * 
     * @param str the string (tile) to put into the index
     * @return the index of the string
     */
    public Integer put(String str) {
        Integer index = this.subMap.get(str);
        if( index == null ) {
            this.subMap.put(str, this.curIndex);
            /** 
             * Internalize the String. Useful for lowering memory
             * during the generating concated Vector.
             * 
             * Ideally, we shouldn't need to do this. 
             * However, if this class isn't used correctly then 
             * this will be a source of memory leaks.
             */
            this.idxMap.put(this.curIndex, str.intern());
           
            //this.idxCnt.put(this.curIndex, 1);
            index = this.curIndex++;
        } else {
            /** Update count */
           // this.idxCnt.put(index, this.idxCnt.get(index) + 1);
          //  System.err.printf("SeqCompDistIndex.put: found '%s'\n", str);
        }
        return index;
    }
    
    /**
     * Return the index for this string.
     * 
     * @param str the tile (sub-string)
     * @return
     */
    public Integer getIndex(String str) {
        return this.subMap.get(str);
    }
            
    /**
     * Return the string with the given index
     * @param index
     * @return String
     */
    public String getString(Integer index) {
        return this.idxMap.get(index);
    }
    
    /**
     * 
     * @return the number of strings indexed
     */
    public Integer size() {
        return this.curIndex - 1;
    }
    
    public Set getStrings() {
        return this.subMap.keySet();
    }
    
    /**
     * Need iterators: stringIterator and indexIterator
     */
}

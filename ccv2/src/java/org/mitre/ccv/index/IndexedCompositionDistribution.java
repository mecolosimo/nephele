/**
 * IndexedCompositionDistribution.java
 *
 * Created on May 4, 2008, 2:50:22 PM
 *
 * $Id$
 */
package org.mitre.ccv.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.mitre.ccv.CompositionDistribution;
import org.mitre.ccv.CompositionDistributionMap;

/**
 * A {@link CompositionDistribution} backed by a {@link DistributionIndex}.
 * <P>
 * This will generate all of the distributions from start - 2 to end 
 * (inclusive).
 *
 * @author Marc Colosimo
 */
public class IndexedCompositionDistribution implements CompositionDistribution {
    /** List of our various maps (windowSize-this.start) */
    private List<IndexedCompositionDistributionMap> countMap;
    
    private final Integer begin;
    private final Integer end;
    
    /** The first windowSize we use */
    private final Integer beginOffset;
    
    private final Integer seqId;
    
    //private Integer size;           // How many counted nmers
    private final DistributionIndex distIndex;
    
    /** Length of the sequence(s) that was used to generate the distribution.  */
    private Integer seqLength;
    
    /**
     * Construct a new <tt>IndexedCompositionDistribution</tt> object.
     * 
     * @param inSequence
     * @param begin
     * @param end
     * @param index the backing DistributionIndex (<code>null</code> allowed 
     * a new index will be created).
     * @throws java.lang.IllegalArgumentException
     */
    public IndexedCompositionDistribution(DistributionIndex index, 
            Integer seqId, String inSequence, 
            int begin, int end) 
    throws IllegalArgumentException {
        if( begin <= 2 ) 
            throw new IllegalArgumentException("Value of 'begin' argument must be larger than 2");
        if( end > inSequence.length() )
            throw new IllegalArgumentException("Value of 'end' argument must be smaller than the length (" 
                    + inSequence.length() + ") of the sequence");
        
        if (index == null)
            this.distIndex = new DistributionIndex();
        else 
            this.distIndex = index;
        
        this.begin = begin;
        this.end = end;
        this.seqId = seqId;
        this.seqLength = inSequence.length();
        
        this.countMap = new ArrayList<IndexedCompositionDistributionMap>();
        
        // probably slowest way to build this.
        this.beginOffset = this.begin - 2;
        for(int i=this.beginOffset; i <= this.end; i++) {
            this.countMap.add(getDistribution(inSequence, null, i));
        }
    }

    public Integer getSeqId() {
        return this.seqId;
    }
    
    /**
     * Return an iterator for the window sizes.
     */
    public Iterator<Integer> iterator() {
        //why did I make this method and why doesn't Java have a range class?
        LinkedList<Integer> ll = new LinkedList<Integer>();
        for (Integer i = this.startingWindowSize(); 
                 i <= this.endingWindowSize();
                 i++ ) {
            ll.add(i);
        }
        return ll.iterator();
    }
    
    /**
     * Add the given sequence to this distribution.
     * 
     * This <B>DOES NOT</B> handle the boundary cases between 
     * the two sequences.
     * 
     * @param inSequence sequence string to add
     */
    public void addSequence(String inSequence) {
        for (Integer cv=0; cv < this.countMap.size(); cv++ ) {
            getDistribution(inSequence, this.countMap.get(cv), cv + this.begin);
        }
        this.seqLength += inSequence.length();
        //this.totalSubStr += insequence.length() - length + 1;
    }
    
    public void addDistribution(CompositionDistribution cd) 
            throws IllegalArgumentException {
        
        for( Iterator<Integer> iter = cd.iterator();
             iter.hasNext(); ) {
            
            Integer ws = iter.next();
            CompositionDistributionMap cdm = this.countMap.get(ws);
            if( cdm != null ) {
                cdm.addMap(cd.getDistribution(ws));
            } else {
                /** We don't have this window size */
                throw new IllegalArgumentException("IndexedCompositionDistribution.addDistribution: " + "" +
                        "Given distribution has additional window size (" + ws + ")");
            
            }
        }
        this.seqLength+=cd.length();
    }

    /**
     * Returns the ditribution for the given window size.
     * 
     * @param windowSize the length of the window (tile).
     * @return the distribution map
     */
    public CompositionDistributionMap getDistribution(Integer windowSize) {
        return this.countMap.get(windowSize - this.beginOffset);
    }

    public Integer length() {
        return this.seqLength;
    }

    /**
     * For single sequence:
     *      L - k + 1
     * where k = the windowSize.
     * 
     */
    public Integer getTotalSubStrings(Integer windowSize) {
        return this.seqLength - windowSize + 1;
    }

    public Integer startingWindowSize() {
        return this.begin;
    }

    public Integer endingWindowSize() {
        return this.end;
    }
    
    public DistributionIndex getDistributionIndex() {
        return this.distIndex;
    }
    
    /**
     * Populate a distribution map with the counts for each substring
     * 
     * @param sequence the sequence
     * @param length the of the nmer to generate counts for
     */
    private IndexedCompositionDistributionMap 
            getDistribution(String sequence, 
                IndexedCompositionDistributionMap cdMap,
                int windowSize) 
                throws IllegalArgumentException {
        
        if( cdMap == null ) {
            cdMap = new 
               IndexedCompositionDistributionMap(windowSize, 
                        this.distIndex, this.seqId, sequence);
        } else {
            cdMap.addSequence(sequence);
        }

        return cdMap;
    }
}

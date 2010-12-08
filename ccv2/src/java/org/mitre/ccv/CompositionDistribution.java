/**
 * CompositionDistribution.java
 * 
 * Created on 4 Jan 2008
 * 
 * $Id$
 */

package org.mitre.ccv;

import java.util.Iterator;

/**
 * Interface for classes that hold information about a sequence full
 * composition distribution.
 *
 * @author Marc Colosimo
 */
public interface CompositionDistribution {

    /**
     * Add the given sequence to this distribution.
     * 
     * This <B>DOES NOT</B> have to handle the boundary cases between 
     * the two sequences.
     * 
     * @param inSequence sequence string to add
     */
    public void addSequence(String inSequence) ;
    
    public void addDistribution(CompositionDistribution scd) throws
            IllegalArgumentException ;
    
    /**
     * Returns the ditribution for the given window size.
     * 
     * @param windowSize the length of the window (tile).
     * @return the distribution map
     */
    public CompositionDistributionMap getDistribution(Integer windowSize) ;
    
    /**
     * The length of the sequence.
     */
    public Integer length();
    
    /**
     * For single sequence:
     *      L - k + 1
     * where k = the windowSize.
     * 
     */
    public Integer getTotalSubStrings(Integer windowSize);
    
    public Integer startingWindowSize();
    
    public Integer endingWindowSize() ;
    
    /**
     * Return an iterator for the window sizes.
     */
    public Iterator<Integer> iterator();
}

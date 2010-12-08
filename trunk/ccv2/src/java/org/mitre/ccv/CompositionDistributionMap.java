/**
 * Created on 4 Jan 2008
 * 
 * $Id$
 */

package org.mitre.ccv;

import java.util.Iterator;

/**
 * Interface for classes holding composition distribution for a particular window size.
 * 
 * @author Marc Colosimo
 */
public interface CompositionDistributionMap {
    
    /**
     * Returns the window size for this distribution map.
     */
    public Integer getWindowSize();
    
    /**
     * Add the string (tile) and increasing its count.
     * 
     * @param str
     * @return boolean true if successful
     */
    public boolean put(String str);
    
    /**
     * Add the sequence to this distribution map.
     */
    public void addSequence(String inSequence);
    
    /**
     * Add the given distribution to this distribution. 
     * @param map
     * @throws java.lang.IllegalArgumentException
     */
    public void addMap(CompositionDistributionMap map) 
            throws IllegalArgumentException;
    
    /**
     * Return the count of the string (tile).
     * 
     * @param str
     * @return
     */
    public Integer get(String str) ;
    
    /**
     * Return an interator for all strings indexed.
     */
    public Iterator<String> iterator();
    
    /**
     * Returns the size (number) of strings we have counted.
     */
    public Integer size();
    
}

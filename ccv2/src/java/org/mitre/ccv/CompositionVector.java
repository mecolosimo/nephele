/**
 * CompositionVector.java
 * 
 * Created on 9 Jan 2008
 * 
 * $Id$
 */

package org.mitre.ccv;

import java.util.Map;
import java.util.Set;

/**
 * An interface for describing a composition vector, as defined in (Which article?)
 * 
 * @author Marc Colosimo
 */
public interface CompositionVector {
    
    /**
     * Returns the size of the window (tile).
     */
    public int getWindowSize() ;

    /**
     * Returns the pi-value for the nmer (tile).  
     */
    public Double getPiValueForNmer(String nmer) ;
    
    /**
     * Returns the set of nmers
     */     
    public Set<String> getNmers() ;
    
    /**
     * Returns the underlining sequence composition distribution.
     */
    public CompositionDistribution getCompositionDistribution() ;
    
    /**
     * Returns the pi values for the composition vector.
     * 
     * This is might be a copy of the data or not. So <B>DO NOT</B> modify it.
     */
    public Map<String, Double> getCompositionVector();
   
}

/**
 * IndexedCompositionVector.java
 *
 * Created on May 4, 2008, 3:01:16 PM
 *
 * $Id$
 */
package org.mitre.ccv.index;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.mitre.ccv.AbstractCompositionVector;
import org.mitre.ccv.CompositionDistribution;
import org.mitre.ccv.CompositionDistributionMap;

/**
 * A CompositionVector class backed by a DistributionIndex.
 * This provides an interface to a single sequence composition over one window size.
 * 
 * @author Marc Colosimo
 */
public class IndexedCompositionVector extends AbstractCompositionVector {
    private final IndexedCompositionDistribution cd;
    private final int windowSize;
    
    public IndexedCompositionVector(int windowSize, IndexedCompositionDistribution distribution) {
        this.cd = distribution;
        this.windowSize = windowSize;
        Map <String, Double> cv = this.createCompositionVector();
        this.storePiValues(cv);
    }
    
    public int getWindowSize() {
        return this.windowSize;
    }

    /**
     * Returns the pi-value for the nmer (tile).  
     */
    public Double getPiValueForNmer(String nmer) {
        return this.cd.getDistributionIndex().getPiValueForNmer(this.cd.getSeqId(), nmer);
    }

    /**
     * Returns all the nmers seen in this window size
     */
    public Set<String> getNmers() {
        return this.cd.getDistributionIndex().getSequenceNmers(this.cd.getSeqId(), this.windowSize);
    }

    public CompositionDistribution getCompositionDistribution() {
        return this.cd;
    }

    public Map<String, Double> getCompositionVector() {
        float loadFactor = (float) 0.75;
        CompositionDistributionMap cm = this.cd.getDistribution(this.windowSize);
        int initialCapacity = (int)((cm.size() + 1) * (1/loadFactor));
    	HashMap<String, Double> compVector = 
                new HashMap<String, Double>(initialCapacity, loadFactor);
        for(Iterator<String> iter=cm.iterator(); iter.hasNext(); ) {
            String nmer = iter.next();
            compVector.put(nmer, this.getPiValueForNmer(nmer));
        }
        return compVector;
    }

    /**
     * Store the piValues in the DistributionIndex
     */
    private void storePiValues(Map <String, Double> cv) {
        for(Entry<String, Double> entry : cv.entrySet()) {
           this.cd.getDistributionIndex().setPiValueForNmer(
                   this.cd.getSeqId(), entry.getKey(), entry.getValue());
        }
    }
}

/**
 * AbstractCompositionVector.java
 *
 * Created on Jan 9, 2008, 10:39:09 AM
 *
 * $Id$
 */
package org.mitre.ccv;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An abstract class for a composition vector, as defined in (Which article?)
 * 
 * @author Marc Colosimo
 * @author Matt Peterson
 */
public abstract class AbstractCompositionVector implements CompositionVector {

    private static final Log LOG = LogFactory.getLog(AbstractCompositionVector.class);

    /**
     * Calculates the pi values for composition vector
     */
    protected Map<String, Double> createCompositionVector() {

        CompositionDistribution cd = this.getCompositionDistribution();
        Integer windowSize = this.getWindowSize();
        Integer totalSubStr = cd.getTotalSubStrings(windowSize);

        final CompositionDistributionMap countsM0 = cd.getDistribution(windowSize);
        final CompositionDistributionMap countsM1 = cd.getDistribution(windowSize - 1);
        final CompositionDistributionMap countsM2 = cd.getDistribution(windowSize - 2);

        /**
         * HashMaps representing the composition vector
         */
        float loadFactor = (float) 0.75;
        int initialCapacity = (int) ((countsM0.size() + 1) * (1 / loadFactor));

        HashMap<String, Double> compVector =
                new HashMap<String, Double>(initialCapacity, loadFactor);

        Iterator<String> iter = countsM0.iterator();
        while (iter.hasNext()) {

            String nmer = iter.next();

            String s1 = nmer.substring(0, windowSize - 1);
            String s2 = nmer.substring(1, windowSize);
            String s3 = nmer.substring(1, windowSize - 1);
            /*
            double p = ((double) countsM0.get(nmer))/(totalSubStr);
            double p1 = ((double) countsM1.get(s1))/(totalSubStr + 1);
            double p2 = ((double) countsM1.get(s2))/(totalSubStr + 1);
            double p3 = ((double) countsM2.get(s3))/(totalSubStr + 2);

            double pe = (p1*p2)/p3;
            double value = (p - pe)/pe;

            compVector.put(nmer, value);
             */
            compVector.put(nmer, calculatePiValue(
                    countsM0.get(nmer), countsM1.get(s1),
                    countsM1.get(s2), countsM2.get(s3),
                    totalSubStr));
        }

        if (LOG.isDebugEnabled() && (compVector.size() > initialCapacity)) {
            LOG.debug(String.format("AbstractCompositionVector.createCompositionVector: " +
                    "compVector size (%d) larger than initialCapacity (%d) for windowSize %d\n",
                    compVector.size(), initialCapacity, windowSize));
        }
        return compVector;
    }

    /**
     * Calculates the pi-value
     * 
     * @param cnt count for base substring
     * @param cnt1 count for substring-1
     * @param cnt2 count for +1 to length substring
     * @param cnt3 count for +1 to substring-1
     * @param totalSubStr total number of substrings of this length
     * 
     * @return the pi-value
     */
    static public Double calculatePiValue(
            int cnt, int cnt1,
            int cnt2, int cnt3,
            int totalSubStr) {

        double p = ((double) cnt) / totalSubStr;
        double p1 = ((double) cnt1) / (totalSubStr + 1);
        double p2 = ((double) cnt2) / (totalSubStr + 1);
        double p3 = ((double) cnt3) / (totalSubStr + 2);

        double pe = (p1 * p2) / p3;
        double value = (p - pe) / pe;
        return value;
    }
}

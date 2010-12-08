/*
 * Created on 13 December 2009.
 *
 * $Id$
 *
 * $Lincese$
 */

package org.mitre.clustering.canopy;

import java.util.ArrayList;
import java.util.List;

/**
 * Places samples into canopies using the {@link CanopyDistanceMetric} and thresholds.
 * Based on McCallum, Nigam and Ungar: "Efficient Clustering of High Dimensional Data Sets with Application to Reference Matching"
 * 
 * @see http://www.kamalnigam.com/papers/canopy-kdd00.pdf
 * @author Marc Colosimo
 */
public class CanopyCluster<T> {

    private CanopyDistanceMetric<T> distanceMetric;
    private float t1;
    private float t2;

    public CanopyCluster(float threshold1, float threshold2, CanopyDistanceMetric<T> metric ) {
        assert(threshold1 > threshold2);
        this.t1 = threshold1;
        this.t2 = threshold2;
        this.distanceMetric = metric;
    }

    /**
     * This generates the canopies from the given list of samples.
     * 
     * @param samples
     * @param randomize randomly pick points to use for canopies (not working yet).
     * @return a list of canopies ({@link Canopy})
     */
    public List<Canopy<T>> cluster(List<T> samples, boolean randomize) {
        ArrayList<Canopy<T>> clusters = new ArrayList<Canopy<T>>();
        ArrayList<T> remaining = new ArrayList<T>(samples);

        while (remaining.size() > 0) {
            // Randomly choose the next canopy?
            T subject = remaining.get(0);
            Canopy<T> curCanopy =  new Canopy<T>(subject);
            clusters.add( curCanopy );
            remaining.remove(0);
            ArrayList<T> removed = new ArrayList<T>();
            for(T sample : remaining) {
                double distance = this.distanceMetric.distance(subject, sample);
                System.err.printf("distance=%f\n", distance);
                if (distance <= this.t2)
                {
                   curCanopy.add(sample);
                   // remaining.remove(sample); ConcurrentModificationException
                   removed.add(sample);
                   System.err.printf("Removing sample %d\n", remaining.indexOf(sample));
                } else if (distance <= this.t1) {
                    curCanopy.add(sample);
                    System.err.printf("Adding sample %d\n", remaining.indexOf(sample));
                }
            }
            // clean up our remaining list
            for(T sample : removed) {
                remaining.remove(sample);
            }
        }
        return clusters;
    }
}

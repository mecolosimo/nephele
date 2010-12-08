/**
 * Created on 15 December 2009.
 *
 * Copyright 2010- The MITRE Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you  may not 
 * use this file except in compliance with the License. You may obtain a copy of 
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions andlimitations under
 * the License.
 *
 * $Id$
 */
package org.mitre.ccv.canopy;

import org.apache.commons.math.linear.RealMatrix;
import org.mitre.ccv.CompleteMatrix;

import org.mitre.clustering.canopy.CanopyDistanceMetric;

/**
 * Tanimoto distances generated by using the Jaccard similaritie of two CCV vectors.
 *
 * This only check to see if the two vectors have either both have non-zero OR zero entries for the same position.
 *
 * @author Marc Colosimo
 */
public class TanimotoCcvDistanceMetric implements  CanopyDistanceMetric<Integer> {

    private CompleteMatrix completeMatrix;

    /**
     * Construct metric for given CompleteMatric
     */
    public TanimotoCcvDistanceMetric(CompleteMatrix matrix) {
        this.completeMatrix = matrix;
    }

    /**
     * The two vectors need to be the same size.
     * @param n1
     * @param n2
     * @return
     */
    public double distance(Integer v1, Integer v2) {
        // check that n1 and n2 are valid sample indices
        return 1 - this.computeSimilarityFromCcv(v1, v2);
    }

    private double computeSimilarityFromCcv(final Integer n1, final Integer n2) {
        // features/kmers are rows (m), samples are columns (n)
        int identicalKmers = 0;
        //int numKmers = n1.getDimension();
        RealMatrix matrix = this.completeMatrix.getMatrix();
        int numKmers = matrix.getRowDimension();
        for (int m = 0; m < numKmers; m++) {
            final double v1 = matrix.getEntry(m, n1);
            final double v2 = matrix.getEntry(m, n2);
            if (v1 == 0 && v2 == 0) {
                identicalKmers++;
            } else if (v1 != 0 && v2 != 0) {
                identicalKmers++;
                //System.err.println("Non-zero matches");
            }
        }
        return (1.0 * identicalKmers) / numKmers;
    }
}

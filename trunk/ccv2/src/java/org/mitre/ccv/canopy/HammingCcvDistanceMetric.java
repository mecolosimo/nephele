/**
 * Created on 23 December 2009.
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
 * A simple Hamming distance metric that counts features as being identical
 * if they are either both positive or negative. If either on is zero it is not counted.
 * 
 * @author Marc Colosimo
 */
public class HammingCcvDistanceMetric implements  CanopyDistanceMetric<Integer> {
    private CompleteMatrix completeMatrix;

    public HammingCcvDistanceMetric(CompleteMatrix matrix) {
        this.completeMatrix = matrix;
    }

     /**
     * The two vectors need to be the same size.
     */
    public double distance(Integer v1, Integer v2) {
        return this.computeHammingFromCcv(v1, v2);
    }

     private double computeHammingFromCcv(final int n1, final int n2) {
        // features/kmers are rows (m), samples are columns (n)
        int identicalKmers = 0;
        //int numKmers = n1.getDimension();
        RealMatrix matrix = this.completeMatrix.getMatrix();
        int numKmers = matrix.getRowDimension();
        for (int m = 0; m < numKmers; m++) {
            final double v1 = matrix.getEntry(m, n1);
            final double v2 = matrix.getEntry(m, n2);
            // if value > 0 bit = 1
            // if value < 0 bit = 0
            // if value = 0 don't compare
            if (v1 != 0 && v2 != 0) {
                if (v1 > 0 && v2 > 0) {
                    identicalKmers++;
                } else if (v1 < 0 && v2 < 0){
                    identicalKmers++;
                }
                //System.err.println("Non-zero matches");
            }
        }
        return (1.0 * identicalKmers) / numKmers;
    }
}

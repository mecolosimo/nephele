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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.mitre.ccv.CompleteMatrix;
import org.mitre.clustering.canopy.Canopy;
import org.mitre.clustering.canopy.CanopyCluster;
import org.mitre.clustering.canopy.CanopyDistanceMetric;
import org.mitre.spectrum.TH1D;
import org.mitre.spectrum.TSpectrum;

/**
 * Class that implements a Canopy Clustering approach to the generated CCV vectors.
 *
 * @see org.mitre.clustering.canopy.CanopyCluster
 * @author Marc Colosimo
 */
public class CcvCanopyCluster {

    private static final Log LOG = LogFactory.getLog("CcvCanopyCluster");
    private CompleteMatrix completeMatrix;
    private List<Canopy<Integer>> vectorCanopies = null;
    private CanopyDistanceMetric<Integer> cheapMetric = null;
    private float t1;
    private float t2;

    /* Generate list of sample vectors which are unfortunately the columns */
    //private List<RealVector> vectors; // tmp?!?
    public CcvCanopyCluster(CompleteMatrix completeMatrix) {

        this.completeMatrix = completeMatrix;
        //this.cheapMetric = new TanimotoCcvDistanceMetric(completeMatrix);
        this.cheapMetric = new HammingCcvDistanceMetric(completeMatrix);
        this.t1 = 0.0f;
        this.t2 = 0.0f;

        //this.vectors = this.completeMatrix.getVectors();
    }

    /**
     * Sets the thresholds 1 and 2 using MaxLike profile.
     *
     * Issues/Pittfalls:
     * <ol>
     * <ul>t2 might be to small and nothing is removed from the list
     * <ul>t1 might be to large and everything is added to a canopy
     * </ol>
     * @todo: figure out how to select threshold1 (not to big not to small)
     */
    public double[] autoThreshold()throws Exception {
        LOG.info("autoThreshold: Generating distance distribution");
        //SortedMap<Double, Integer> sortMap = new TreeMap<Double, Integer>(new ReverseDoubleComparator());
        SortedMap<Double, Integer> sortMap = new TreeMap<Double, Integer>();
        // generate all the pairwise distances
        final int size = completeMatrix.getMatrix().getColumnDimension();
        for (int i = 0; i < size; ++i) {
            for (int j = i + 1; j < size; ++j) {
                // only calculate one triangle not full!
                Double d = this.cheapMetric.distance(i, j);
                //set.add(this.cheapMetric.distance(i, j));
                if (sortMap.containsKey(d)) {
                    sortMap.put(d, sortMap.get(d) + 1);
                } else {
                    sortMap.put(d, 1);
                }
            }
        }

                           /**
             * $gnuplot
             * > set nokey
             * > set xlabel "Pairwise distance"
             * > set ylabel "Number of samples"
             * > plot "output.txt" using 1:2
*/
        /* */
        for (Iterator<Entry<Double, Integer>> i = sortMap.entrySet().iterator(); i.hasNext();) {
            Entry<Double, Integer> entry = i.next();
            
            //System.out.printf("%f\t%d\n", entry.getKey(), entry.getValue());
        }
      /* */

        /**
         * How many bins per samples do we want?
         * Using the two end cases at lower and upper bounds.
         */
        TH1D hist = new TH1D(completeMatrix.getMatrix().getColumnDimension() * 2,
                sortMap.firstKey(), sortMap.lastKey());
        LOG.info(String.format("autoThreshold: Packing into histogram with %d bins (%f, %f)",
                hist.getBins().length, hist.getLower(), hist.getUpper()));
        hist.pack(sortMap);
        int[] bins = hist.getBins();
        if (LOG.isDebugEnabled()) {
            if (hist.getNumberOverflows() != 0) {
                LOG.debug(String.format("autoThreshold: Have %d overflows in histogram!", hist.getNumberOverflows()));
            }
            if (hist.getNumberUnderflows() != 0) {
                LOG.debug(String.format("autoThreshold: Have %d underflows in histogram!", hist.getNumberUnderflows()));
            }
        }

        // print out histogram bins
        for(int i=0; i < bins.length; i++) {
          //System.out.printf("%f\t%d\n", hist.getBinCenter(i), hist.getBinContent(i));
        }
        TSpectrum spectrum = new TSpectrum();   // use default values (sigma = 1, threshold = 0.5
        int numFound = spectrum.search(hist);
        LOG.info(String.format("autoThreshold: Found %d peaks", numFound));
        if (numFound == 0) {
            LOG.fatal("autoThreshold: No peaks found in data!");
            throw new Exception();
        }
        double xpeaks[] = spectrum.getPostionX();
        double[] rtn = new double[2];  // t1, t2
        if (numFound == 1) {
            int bin = hist.findBin(xpeaks[0]);
            // is this in the top or bottom half?
            // @todo: must be better way than this hack
            if (bin > 0) {
                bin--;      
            }
            rtn[0] = hist.getBinCenter(bin);    // threshold1 is only peak
            rtn[1] = (hist.getLower() + rtn[0]) / 2;
            return rtn;
        }
        
        // more than one peak
        /**
         * Several possible options:
         * - select t1 first than find a good t2
         * - select t2 first than find a good t1
         * 
         * make sure that there is enough samples below t2 and above t1
         
        if (xpeaks[0] > xpeaks[1]) {
            // what about sigma value: how many are between these two
            rtn[0] = xpeaks[0]; // t1
            rtn[1] = xpeaks[1];  //t2
        } else {
            rtn[0] = xpeaks[1];
            rtn[1] = xpeaks[0];
        }
        */

        // find the peak with the smallest this will be the basis for t2
        double minPeakX = hist.getUpper();
        int minPeakI = -1;
        for (int i = 0; i < numFound; i++) {
            final double x = xpeaks[i];
            if (x < minPeakX) {
                minPeakX = x;
                minPeakI = i;
            }
        }
        //System.err.printf("minPeakX=%f (%d)\n", minPeakX, minPeakI);

        // find next peak above the smallest
        // should try using something about the average and standard deviation
        // of the distribution of entries in picking this
        double min2PeakX = hist.getUpper();
        int min2PeakI = -1;
        for (int i = 0; i <numFound; i++) {
            final double x = xpeaks[i];
            if (i != minPeakI && x < min2PeakX) {  // should check that it isn't equal or within sigma
                min2PeakX = x;
                min2PeakI = i;
            }
        }
        //System.err.printf("min2PeakX=%f (%d)\n", min2PeakX, min2PeakI);
        /**
        if (minPeakI + 1 < min2PeakI - 1) {
            rtn[0] = hist.getBinCenter(min2PeakI - 1);         // t1
            rtn[1] = hist.getBinCenter(minPeakI + 1);          // t2
        } else {
            // really close not good - these should be the centers
            LOG.info("autoThreshold: t1 and t2 are possbily from adjacent bins!");
            rtn[0] = min2PeakX;
            rtn[1] = minPeakX;
        }
        int t2bin = hist.findBin(minPeakX);
        if (t2bin - 1 > 0 ) {
            rtn[1] = hist.getBinCenter(t2bin - 1); // don't want the first bin?
        } else {
            rtn[1] = minPeakX;
        }
        int t1bin = hist.findBin(min2PeakX);
        if (t1bin + 1 < bins.length - 1) {  // don't want the last bin?
            rtn[0] = hist.getBinCenter(t1bin + 1);
        } else {
            rtn[0] = min2PeakX;
        }*/

        rtn[0] = min2PeakX;
        rtn[1] = minPeakX;
        
        /*
        double t1 = hist.getUpper();
        double t2 = hist.getLower(); */
        // print out what we found
        for (int p = 0; p < numFound; p++ ){
            double xp = xpeaks[p];
            int bin = hist.findBin(xp);
            int yp = hist.getBinContent(bin); // double yp
            System.err.printf("%d\t%f\t%d\n", bin, xp, yp);
            // if(yp- Math.sqrt(yp) < fline.eval(xp)) continue
        }

        return rtn;
    }

    /**
     * Comparator class that sorts doubles from largest to smallest
     */
    public class ReverseDoubleComparator implements Comparator<Double> {

        public int compare(Double d, Double d1) {
            return -1 * Double.compare(d, d1);
        }
    }

   
    /**
     * Returns the threshold1
     */
    public float getThreshold1() {
        return this.t1;
    }

    /**
     * Returns the threshold2
     */
    public float getThreshold2() {
        return this.t2;
    }

    public void cluster(float threshold1, float threshold2) {
        // need python like range
        ArrayList<Integer> indices = new ArrayList<Integer>();
        for (int n = 0; n < this.completeMatrix.getNames().size(); n++) {
            indices.add(n);
        }
        CanopyCluster<Integer> canopyCluster = new CanopyCluster(threshold1, threshold2, cheapMetric);
        this.vectorCanopies = canopyCluster.cluster(indices, false);
        LOG.info(String.format("Generated %d canopies for %d samples using t1=%f and t2=%f",
                this.vectorCanopies.size(), indices.size(), threshold1, threshold2));
    }

    @Override
    public String toString() {
        // @todo: convert to JSON
        //List<RealVector> vectors = this.completeMatrix.getVectors();
        ArrayList<String> names = this.completeMatrix.getNames();
        StringBuffer sb = new StringBuffer();
        for (Iterator<Canopy<Integer>> ci = this.vectorCanopies.iterator(); ci.hasNext();) {
            Canopy<Integer> v = ci.next();
            sb.append(names.get(v.getFounder()));
            sb.append(": {");
            for (Iterator<Integer> mi = v.iterator(); mi.hasNext();) {
                sb.append(names.get(mi.next()));
                if (mi.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(" }");
            if (ci.hasNext()) {
                sb.append(", \n");
            }
        }
        return sb.toString();
    }

    /**
     * Rough main to test code. Loads in json file and outputs canopies
     * @param arg
     * @throws Exception
     */
    @SuppressWarnings("static-access")
    public static void main(String[] argv) throws Exception {
        if (argv.length >= 1) {
            LOG.info("Reading in CompleteCompositionVectors from " + argv[0]);
            // we only save the data not everything that is in the vectorSet
            BufferedReader br = new BufferedReader(new FileReader(argv[0]));
            CompleteMatrix completeMatrix = CompleteMatrix.readJsonCompleteMatrix(br);
            br.close();
            LOG.info(String.format("Loaded in %d samples and %d nmers (features)",
                    completeMatrix.getNames().size(), completeMatrix.getNmers().size()));
            CcvCanopyCluster canopyCluster = new CcvCanopyCluster(completeMatrix);
            float t1 = (float) -1.0;
            float t2 = (float) -1.0;
            if (argv.length >= 2) {
                t1 = Float.parseFloat(argv[1]);
            }
            if (argv.length >= 3) {
                t2 = Float.parseFloat(argv[2]);
            }
            if (t1 < 0.0 || t2 < 0.0) {
                double[] ts = canopyCluster.autoThreshold();
                t1 = (float) ts[0];
                t2 = (float) ts[1];
            }
            
            canopyCluster.cluster(t1, t2);
            //System.out.println(canopyCluster.toString());
        }
    }
}

/**
 * AbstractVectorSet.java
 *
 * Created on Jan 6, 2008, 2:51:58 PM
 *
 * $Id$
 *
 * Changes from Jan 6, 2008
 * -------------------------
 * 2008-2009   : Lots of changes (mec)
 * 8-Sept-2009 : reworked create*DistanceMatrix functions (mec)
 *
 */
package org.mitre.ccv;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.math.linear.RealMatrix;
import org.mitre.bio.phylo.DistanceMatrix;
import org.mitre.math.linear.RealMatrixUtils;
//import weka.core.matrix.Matrix;


/**
 * Abstract class for a collection (list) of CompleteCompositionVectors
 * calculated over a given range of window sizes.
 * 
 * @author Marc Colosimo
 * @author Matt Peterson
 */
abstract public class AbstractVectorSet implements VectorSet {

    private static final Log LOG = LogFactory.getLog("AbstractVectorSet");

    private final RealMatrixUtils matrixUtils = RealMatrixUtils.getSingleton();

    /**
     * Add the sequence to this set of vectors.
     * 
     * @param seqName
     * @param seq
     */
    abstract public void addSequence(String seqName, String seq);

    /**
     * Returns the list of CompleteCompositionVectors
     */
    abstract public List<CompleteCompositionVector> getVectors();

    /** 
     * Returns the starting window size.
     */
    abstract public Integer getStart();

    /**
     * Returns the ending window size.
     */
    abstract public Integer getStop();

    /**
     * Gets the names of all of the sequences in this set.
     * 
     * @return an array of names
     */
    public ArrayList<String> getSampleNames() {
        ArrayList<String> names = new ArrayList<String>();
        for (CompleteCompositionVector v : this.getVectors()) {
            names.add(v.getName());
        }
        return names;
    }

    /**
     * Calculates the optimal dimensionality using the method outlined in
     * Mu Zhu and Ali Ghodsi (2005) "Automatic dimensionality selection from the scree plot via the use of profile likelihood"
     * Computational Statistics & Data Analysis 51(2):918-930
     * {@link http://www.sciencedirect.com/science/journal/01679473}
     * {@link http://dx.doi.org/10.1016/j.csda.2005.09.010}
     * 
     * @param pairs
     * @param numLookAhead how far to look before returning. 1 returns default. 
     *                     (default 200, <code>null</code> allowed)
     * @return
     */
    public int getNmersByLikelihood(TreeSet<EntropyPair> pairs, Integer numLookAhead) {
        // Iterate over the set
        int count = 0;
        final int paircount = pairs.size();

        int optim = 0;
        double maxLikelihood = -Double.MAX_VALUE;

        /** How far to look before stoping */
        int lookAhead;

        if (numLookAhead != null && numLookAhead > 1) {
            lookAhead = numLookAhead;
        } else {
            lookAhead = 200;
        }

        Iterator<EntropyPair> pairIterator = pairs.iterator();

        if (pairIterator.hasNext()) {
            pairIterator.next();
        }

        while (pairIterator.hasNext()) {
            count++;
            EntropyPair ep = pairIterator.next();
            // Split up the set into two seperate sets
            // Think about using ArrayList and keeping track of begin, split, end
            SortedSet<EntropyPair> l1 = pairs.headSet(ep);
            SortedSet<EntropyPair> l2 = pairs.tailSet(ep);

            // Calculate the parameters (mean, variance).  Should this function
            // be moved into another seperate class, to reuse?
            final double[] stats1 = calculateParameters(l1);
            final double[] stats2 = calculateParameters(l2);

            final double u1 = stats1[0];
            final double u2 = stats2[0];
            final double var = ((count - 1) * stats1[1] + (paircount - count - 1) * stats2[1]) / (paircount - 2);

            /*
            double likelihood = 0;
            likelihood += getLikelihood(l1, u1, sigma);
            likelihood += getLikelihood(l2, u2, sigma);
*/
            final double likelihood = getLikelihood(l1, u1, var) + getLikelihood(l2, u2, var);
            if (likelihood > maxLikelihood) {
                maxLikelihood = likelihood;
                optim = count;
            }

            if (count > optim + lookAhead) {
                break;
            }

        /** TODO: add option to save likelihoods */
        //System.out.printf("%f\n", likelihood);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("AbstractVectorSet.getNmersByLikelihood: found %d nmers/n", optim));
        }
        return optim;
    }

    /**
     * Calculates mean (first element) and variance (second element) from the
     * SortedSet
     * 
     * @param pairs
     * @return
     */
    public double[] calculateParameters(SortedSet<EntropyPair> pairs) {
        final double[] statistics = new double[2];

        double mean = 0;
        double s = 0;
        int n = 0;
        Iterator<EntropyPair> i = pairs.iterator();
        while (i.hasNext()) {
            EntropyPair ep = i.next();
            final double x = ep.value;
            n++;
            final double delta = x - mean;
            mean += delta / n;
            s += delta * (x - mean);
        }

        statistics[0] = mean;
        statistics[1] = s / n;

        return statistics;
    }

    /**
     * Distribution function for the likelihood calculation
     * 
     * @param d
     * @param mean
     * @param sigma
     */
    private double gaussianValue(final double d, final double mean, final double sigma) {
        return (1 / (2 * Math.PI * sigma)) * Math.exp(-Math.pow((d - mean), 2) / (2 * sigma));
    }

    /**
     * Calculates the likelihood for a given set
     * @param p
     */
    private double getLikelihood(SortedSet<EntropyPair> epset, double mean, double sigma) {
        double l = 0;

        Iterator<EntropyPair> i = epset.iterator();
        while (i.hasNext()) {
            EntropyPair ep = i.next();
            final double v = ep.value;
            final double g = gaussianValue(v, mean, sigma);
            if (g > 0.0) {
                l += g;
            }
        }

        return l;
    }

    /**
     * Find the m top n-mers with the highest entropy.
     *  
     * @param m if m is zero, this will return the n-mers where the
     *          revised relative entropy is great than or equal to 1;
     *          if m is negative, this will first use the Likelihood Profile
     *          method to find the best n-mers.
     *          <code>null</code> is allowed and will return the full set.
     * @param entFileName the file name to write the entropies out to
     *        (<code>null</code> is allowed and nothing is written out).
     * @return java.util.TreeSet of top m n-mers found
     */
    public TreeSet<String> getNmers(Integer m, String entFileName) {

        TreeSet<String> nmers = this.getNmers();
        if (m == null) {
            LOG.debug(String.format("getNmers(int, string): Returning all '%d' nmers\n", nmers.size()));

            return nmers;
        }
        /** Generate the vector matrix for all sequences. This is 1-D. */
        CompleteMatrix sMtx = this.getCompleteMatrix(nmers);
        TreeSet<EntropyPair> entSet = new TreeSet<EntropyPair>();
        ArrayList<String> nmersArray = new ArrayList<String>(nmers);

        CompleteMatrix cmtx = this.getFullMatrix(nmers);
        RealMatrix mtx = cmtx.getMatrix();
        int r = mtx.getRowDimension();
        int c = mtx.getColumnDimension();

        /** This seems numerically stable */
        LOG.debug(String.format("Calculating revised relative entropies (%d by %d)", r, c));
        for (int i = 0; i < r; ++i) {
            //for (int i = r - 1; i >= 0; i--) {
            double ent = 0;
            for (int j = 0; j < c; j++) {
                //for (int j = c - 1; j >= 0; j--) {
                if (mtx.getEntry(i, j) != 0) {
                    ent += Math.abs(mtx.getEntry(i, j)) * Math.log(Math.abs(mtx.getEntry(i, j) / sMtx.get(i, 0)));
                }
            }

            ent = Math.abs(ent);
            EntropyPair p = new EntropyPair(nmersArray.get(i), ent);
            entSet.add(p);
        }
        nmersArray = null;


        if (m < 0) {
            /**
             * If negative then we will use the Likelihood method to find the
             * the best number of top nmers.
             */
            m = this.getNmersByLikelihood(entSet, -1 * m);
        } else if (m == 0) {
            /**
             * If equal to zero, then return all entropies
             */
            m = entSet.size();
        } else if (m > entSet.size()) {
            /**
             * Return all entropies.
             */
            LOG.debug(String.format(
                    "getNmers(int, string): Asked for %d topNmers, but only have %d\n", m, entSet.size()));
            m = entSet.size();
        }

        /** Create top nmer set */
        TreeSet<String> top = new TreeSet<String>();
        int i = 0;
        for (EntropyPair ep : entSet) {

            if (ep.getValue() < 1 && m == 0) {
                break;
            }
            if (i >= m) {
                break;
            }
            top.add(ep.getKey());
            i++;
        }

        if (entFileName != null) {
            writeEntropyFile(entFileName, entSet);
        }

        LOG.debug(String.format("AbstractVectorSet.getNmers(int): have %d topNmers\n", top.size()));
        return top;
    }

    /**
     * Export an the Entropies to a file.
     * 
     * @param entName the file name.
     * @param entMap the entropy map.
     */
    public void writeEntropyFile(String entName,
            TreeSet<EntropyPair> entSet) {
        Iterator<EntropyPair> i = entSet.iterator();

        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(entName));
            while (i.hasNext()) {
                EntropyPair p = i.next();
                bw.write(p.key + "\t" + p.value + "\n");
            }

            bw.close();
        } catch (Exception ioe) {
            ioe.printStackTrace();
        }
    }

    /**
     * Creates a distance Matrix from the sequence set using the given nmers.
     * 
     * @return pal.distance.DistanceMatrix representing the distances
     */
    public DistanceMatrix createEuclidianDistanceMatrix(CompleteMatrix completeMatrix) {
        Integer size = completeMatrix.getNames().size();

        DistanceMatrix distMatrix = new DistanceMatrix(new double[size][size],
                new String[completeMatrix.getNames().size()]);

        RealMatrix matrix = completeMatrix.getMatrix();
        matrixUtils.normalizeMatrix(matrix);

        int nmerSize = matrix.getRowDimension();
        for (int i = 0; i < size; ++i) {
            distMatrix.setIdentifier(i, completeMatrix.getNames().get(i));
            RealMatrix mI = matrix.getSubMatrix(0, nmerSize - 1, i, i);
            for (int j = i + 1; j < size; ++j) {
                if (j == i) {
                    distMatrix.setDistance(i, j, 0);
                } else {
                    RealMatrix mJ = matrix.getSubMatrix(0,  nmerSize - 1, j, j);
                    RealMatrix mT = mI.subtract(mJ);  //mI.minus(mJ);
                    mT = matrixUtils.arrayTimes(mT, mT);
                    double dist = Math.sqrt(matrixUtils.norm1(mT));
                    distMatrix.setDistance(i, j, dist);
                }
            }
        }
        return distMatrix;
    }

    /**
     * Creates a Manhattan distance matrix
     * 
     * @param matrix weka.core.matrix.Matrix
     * @return pal.distance.DistanceMatrix representing the distances
     */
    public DistanceMatrix createMHDistanceMatrix(CompleteMatrix completeMatrix) {
        Integer size = completeMatrix.getNames().size();

        DistanceMatrix distMatrix = new DistanceMatrix(new double[size][size],
                new String[completeMatrix.getNames().size()]);

        RealMatrix matrix = completeMatrix.getMatrix();
        matrixUtils.normalizeMatrix(matrix);

        int nmerSize = matrix.getRowDimension();//might be column?
        int[] rowInds = new int[nmerSize];
        for (int i = 0; i < nmerSize; ++i) {
            rowInds[i] = i;
        }

        for (int i = 0; i < size; ++i) {
            distMatrix.setIdentifier(i, completeMatrix.getNames().get(i));
            RealMatrix mI = matrix.getSubMatrix(0, nmerSize - 1, i, i);
            for (int j = i + 1; j < size; ++j) {
                if (j == i) {
                    distMatrix.setDistance(i, j, 0);
                } else {
                    RealMatrix mJ = matrix.getSubMatrix(0, nmerSize - 1, j, j);
                    RealMatrix mT = mI.subtract(mJ);         //mI.minus(mJ);
                    double dist = matrixUtils.norm1(mT);     //mT.norm1();
                    distMatrix.setDistance(i, j, dist);
                }
            }
        }
        return distMatrix;
    }

    /**
     * Creates a squared Euclidian distance matrix
     * 
     * @param matrix weka.core.matrix.Matrix
     * @return pal.distance.DistanceMatrix representing the distances
     */
    public DistanceMatrix createESDistanceMatrix(CompleteMatrix completeMatrix) {
        Integer size = completeMatrix.getNames().size();
        DistanceMatrix distMatrix = new DistanceMatrix(new double[size][size],
               new String[completeMatrix.getNames().size()]);

        RealMatrix matrix = completeMatrix.getMatrix();
        matrixUtils.normalizeMatrix(matrix);

        int nmerSize = matrix.getRowDimension();
        for (int i = 0; i < size; ++i) {
            distMatrix.setIdentifier(i, completeMatrix.getNames().get(i));
            RealMatrix mI = matrix.getSubMatrix(0, nmerSize - 1, i, i);
            for (int j = i + 1; j < size; ++j) {
                if (j == i) {
                    distMatrix.setDistance(i, j, 0);
                } else {
                    RealMatrix mJ = matrix.getSubMatrix(0, nmerSize - 1, j, j);
                    RealMatrix mT = mI.subtract(mJ);
                    mT = matrixUtils.arrayTimes(mT, mT);
                    distMatrix.setDistance(i, j, matrixUtils.norm1(mT));
                }
            }
        }
        return distMatrix;
    }

    /**
     * Creates a cosine distance matrix
     * 
     * @param CompleteMatrix with rows being nmers and columns being samples
     * @return pal.distance.DistanceMatrix representing the distances
     */
    public DistanceMatrix createCosineDistanceMatrix(CompleteMatrix completeMatrix) {
        Integer size = completeMatrix.getNames().size();
        DistanceMatrix distMatrix = new DistanceMatrix(new double[size][size],
                new String[completeMatrix.getNames().size()]);

        RealMatrix matrix = completeMatrix.getMatrix();
        matrixUtils.normalizeMatrix(matrix);

        int nmerSize = matrix.getRowDimension();
        for (int i = 0; i < size; ++i) {
            distMatrix.setIdentifier(i,completeMatrix.getNames().get(i));
            RealMatrix mI = matrix.getSubMatrix(0, nmerSize - 1, i, i);
            RealMatrix mIT = mI.transpose();
            for (int j = i + 1; j < size; ++j) {
                if (j == i) {
                    distMatrix.setDistance(i, j, 0);
                } else {
                    RealMatrix mJ = matrix.getSubMatrix(0, nmerSize - 1, j, j);
                    Double dot = mIT.multiply(mJ).getEntry(0, 0);
                    dot = dot / (matrixUtils.norm2(mI) * matrixUtils.norm2(mJ));
                    dot = (1.0 - dot) / 2.0;
                    if (dot < 0) {
                        dot = 0.0;
                    }
                    distMatrix.setDistance(i, j, dot);
                }
            }
        }

        return distMatrix;
    }

    /**
     * Creates a Jaccard distance matrix.
     *
     *@param an optional list (set) of nmers to calculate from (intersection).
     *       If <code>null</code> or empty then it will use all nmers it finds.
     *
     * @return {@link DistanceMatrix} representing the distances
     */
    public DistanceMatrix createJaccardDistanceMatrix(ArrayList<String> nmerList) {
        Set<String> nmerSet = null;
        if (nmerList != null && !nmerList.isEmpty()) {
            nmerSet = new HashSet<String>(nmerList);
            if (LOG.isDebugEnabled()) {
                if (nmerSet.size() != nmerList.size()) {
                    LOG.debug("Given nmerList contained duplicate nmers!");
                }
                LOG.debug(String.format("createJaccardDistanceMatrix: Using a base set of %d nmers", nmerSet.size()));
            }
        }
        
        List<CompleteCompositionVector> vectors = this.getVectors();
        Integer size = vectors.size();
        DistanceMatrix distMatrix = new DistanceMatrix(new double[size][size],
                new String[size]);

        // Cui intersect Cuj count over Cui union Cuj
         for (int i = 0; i < vectors.size(); ++i) {
             CompleteCompositionVector ccvi = vectors.get(i);
             distMatrix.setIdentifier(i,ccvi.getName());
             Set<String> Cui;
             if (nmerSet == null) {
                 Cui = ccvi.getNmerSet();
             } else {
                 Cui = new HashSet<String>(ccvi.getNmerSet());
                 Cui.retainAll(nmerSet);
             }
             for (int j = i + 1; j < vectors.size(); ++j) {
                 if (j == i) {
                     distMatrix.setDistance(i,j, 0);
                 } else {
                     CompleteCompositionVector ccvj = vectors.get(j);
                     Set<String> Cuj;
                     if (nmerSet == null) {
                         Cuj = ccvj.getNmerSet();
                     } else {
                         Cuj = new HashSet<String>(ccvj.getNmerSet());
                         Cuj.retainAll(nmerSet);
                     }
                     
                     Set<String> intersection = new HashSet<String>(Cui);
                     intersection.retainAll(Cuj);

                     Set<String> union = new HashSet<String>(Cui);
                     union.addAll(Cuj);
                     
                     Double sim = (double) intersection.size()/union.size();
                     distMatrix.setDistance(i, j, 1-sim);
                 }
             }
         }

        return distMatrix;
    }

    /**
     * Class used for sorting entropy key-value pairs by value
     * 
     * @author Matt Peterson
     */
    public class EntropyPair implements Comparable<EntropyPair> {

        private String key;
        private Double value;

        public EntropyPair(String k, Double v) {
            key = k;
            value = v;
        }

        public int compareTo(EntropyPair p) {
            int cmp = Double.compare(value, p.value);
            if (cmp != 0) {
                return cmp * -1;
            }
            return key.compareTo(p.key);
        }

        @Override
        public String toString() {
            return key + ":" + value;
        }

        public String getKey() {
            return key;
        }

        public Double getValue() {
            return value;
        }

        public Double addValue(Double ent) {
            value += ent;
            return value;
        }

        public void setValue(Double ent) {
            value = ent;
        }
    }
}

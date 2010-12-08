/**
 * IndexedCompleteCompositionVectorSet.java
 *
 * Created on May 4, 2008, 4:02:13 PM
 *
 * $Id$
 */
package org.mitre.ccv.index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.math.linear.RealMatrix;

import org.mitre.ccv.AbstractCompositionVector;
import org.mitre.ccv.AbstractVectorSet;
import org.mitre.ccv.CompleteCompositionVector;
import org.mitre.ccv.CompleteMatrix;
import org.mitre.math.linear.RealMatrixUtils;

// because of IndexCompelteCompositionVector (and CompositionVector Interface)
import weka.core.matrix.Matrix;

/**
 * Class that that is a collection (list) of <code>CompleteCompositionVectors</code>
 * calculated over a given range of window sizes. The composition is backed by
 * <code>DistributionIndex</code>. This keeps track of the sequences, but
 * does <b>NOT</b> store the original sequence.
 * 
 * @warm This class does not store the sequence.
 * @author Marc Colosimo
 */
public class IndexedCompleteCompositionVectorSet extends AbstractVectorSet {

    private static final Log LOG = LogFactory.getLog("IndexedCompleteCompositionVectorSet");

    private List<CompleteCompositionVector> vectors;
    private final Integer start;
    private final Integer stop;
    private final List<String> sequences;
    private final DistributionIndex distIndex;
    
    /**
     * Construct a new <tt>IndexedCompleteCompositionVectorSet</tt> object.
     * 
     * @param start starting nmer size
     * @param stop ending nmer size
     */
    public IndexedCompleteCompositionVectorSet(Integer start, Integer stop) {
        this.vectors = new ArrayList<CompleteCompositionVector>();
        this.start = start;
        this.stop = stop;
        this.sequences = new LinkedList<String>();
        this.distIndex = new DistributionIndex();
    }

    /**
     * Add a new sequence and calculate the complete composition.
     *
     * @param seqName
     * @param seq
     */
    public void addSequence(String seqName, String seq) {
        this.sequences.add(seqName);
        Integer seqId = this.sequences.indexOf(seqName);
        IndexedCompositionDistribution cd = 
                new IndexedCompositionDistribution(this.distIndex, seqId, seq,
                    this.start, this.stop);
        
        CompleteCompositionVector ccv =
                new IndexedCompleteCompositionVector(seqName, seqId,
                this.start, this.stop, cd);
        this.vectors.add(ccv);
    }

    /**
     * Returns the {@link java.util.List} of {@link CompleteCompositionVectors}
     */
    public List<CompleteCompositionVector> getVectors() {
        return this.vectors;
    }

    /** 
     * Returns the starting window size.
     */
    public Integer getStart() {
        return this.start;
    }

    /**
     * Returns the ending window size.
     */
    public Integer getStop() {
        return this.stop;
    }
    
    /**
     * Returns the <code>TreeSet</code> of non-zero n-mers.
     *
     * @return java.util.TreeSet of n-mers found in at least one sequence
     */
    public TreeSet<String> getNmers() {
        /** Faster adding to HashSet and converting to TreeSet */
        HashSet<String> nmers = new HashSet<String>();
        Iterator<CompleteCompositionVector> ccvIter = vectors.iterator();

        while (ccvIter.hasNext()) {
            CompleteCompositionVector ccv = ccvIter.next();
            nmers.addAll(ccv.getNmerSet());
        }
        LOG.debug(String.format(
                "IndexedCompleteCompositionVectorSet.getNmers(): found %d nmers"
                ,nmers.size()) );
        return new TreeSet<String>(nmers);
    }

    /**
     * Returns a <code>TreeSet</code> with the n-mers with the highest entropy.
     *
     * @param m
     * @return
     */
    public TreeSet<String> getNmers(Integer m) {
        return this.getNmers(m, null);
    }
    
   /**
     * Returns a <code>CompleteMatrix</code> of the non-zero n-mers by sequence
     * (<code>null</code> or empty Set is allowed and returns a matrix with all the nmers).
     *
     * @param nmers the set of nmers to build the matrix from.
     */
    public CompleteMatrix getFullMatrix(TreeSet<String> nmers) {
        if(nmers == null || nmers.isEmpty() ) 
            nmers = this.getNmers();
        LOG.debug(String.format("getFullMatrix getting matrix for %d nmers", nmers.size()));

        RealMatrix matrix = RealMatrixUtils.getNewRealMatrix(nmers.size(), this.vectors.size());

        /*int[] rowInds = new int[nmers.size()];
        for (int i = 0; i < nmers.size(); ++i) {
            rowInds[i] = i;
        }*/

        for (int i = 0; i < vectors.size(); i++) {
            // @TODO: fix IndexCompleteCompositionVector to keep the matrix
            Matrix vm = this.vectors.get(i).getMatrix(nmers);
            for (int j = 0; j < nmers.size(); j++) {
                matrix.setEntry(j, i, vm.get(j, 0));
            }
        }
        return new CompleteMatrix(this.start, this.stop,
                new ArrayList<String>(nmers), 
                this.getSampleNames(), matrix);
    }
    
    /**
     * Returns a CompleteMatrix of the non-zero n-mers by sequence
     * (<code>null</code> or zero is allowed and this 
     * will return a matrix with all the nmers).
     * 
     * @param topNmers the top number of n-mers (by
     * @param entFileName the name of the file to write the entropies to.
     *        (<code>null</code> is allowed).
     * @return CompleteMatrix
     * @see AbstractVectorSet.getNmers()
     */
    public CompleteMatrix getFullMatrix(Integer topNmers, String entFileName) {
        TreeSet<String> set = this.getNmers(topNmers, entFileName);
        return this.getFullMatrix(set);
    }

    /**
     * Returns a CompleteMatrix of the non-zero n-mers by sequence
     * (<code>null</code> or zero is allowed and this
     * will return a matrix with all the nmers).
     * @param topNmers
     */
    public CompleteMatrix getFullMatrix(Integer topNmers) {
        return this.getFullMatrix(topNmers, null);
    }
   
    /**
     * Returns a <code>CompleteMatrix<code> of the pi-values for each
     * n-mer using all of the sequences.
     * <P>This is a one-dimensional matrix</P>
     * 
     * @param nmers the nmers to build the matrix from
     */
    public CompleteMatrix getCompleteMatrix(Set<String> nmers) {
        LOG.debug(String.format("IndexedCompleteCompositionVectorSet." +
                "getCompleteMatrix(Set<String>): Building matrix " +
                "from %d nmers.\n", nmers.size()) );
        ArrayList<String> nmersAL = new ArrayList<String>(nmers);
        int count = nmersAL.size();
        RealMatrix matrix = RealMatrixUtils.getNewRealMatrix(count, 1);

        /** 
         * Need to calculate all the totalSubStrs for each window size.
         */
        ArrayList<Integer> totalSubStrs = new ArrayList<Integer>();
        for(int windowSize = this.start; windowSize <= this.stop; windowSize++) {
            Integer cv = 0;
            for(CompleteCompositionVector v: this.vectors) {
               cv += v.getCompositionDistribution().getTotalSubStrings(windowSize);
            }
            totalSubStrs.add(cv);
        }
        
        for (int i = 0; i < count; i++) {
            String nmer = nmersAL.get(i);
            Double value = this.calculateFullPiValue(nmer, 
                    totalSubStrs.get(nmer.length() - this.start));
            if (value == null) {
                value = 0.0;
            }
            //wekaMatrix.set(i, 0, value);
            matrix.setEntry(i, 0, value);
        }

        /** clean-up */
        totalSubStrs.clear();
        totalSubStrs = null;
        
        return new CompleteMatrix(this.start, this.stop, 
                new ArrayList<String>(nmers),
                this.getSampleNames(), matrix); //wekaMatrix);
    }
    
    /**
     * Calculates the pi-value for a given n-mer across all sequences
     * 
     * @param nmer the nmer to use
     * @param totalSubStr total number of sumbstring of this length
     */
    private Double calculateFullPiValue(String nmer, Integer totalSubStr) {
        
        /** check AbstractCompositionVector for errors */
        Integer windowSize = nmer.length();
        String s1 = nmer.substring(0, windowSize - 1);
        String s2 = nmer.substring(1, windowSize);
        String s3 = nmer.substring(1, windowSize - 1);
        
        /** One of these might throw a NULL error */
        Integer countsM0 = this.distIndex.sumCounts(nmer);
        Integer countsM1 = this.distIndex.sumCounts(s1);
        Integer countsM2 = this.distIndex.sumCounts(s2);
        Integer countsM3 = this.distIndex.sumCounts(s3);
        
        return AbstractCompositionVector.calculatePiValue(
                        countsM0, countsM1,
                        countsM2, countsM3,
                        totalSubStr);
    }
}

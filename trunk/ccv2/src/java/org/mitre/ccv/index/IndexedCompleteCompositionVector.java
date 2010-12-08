/**
 * IndexedCompleteCompositionVector.java
 *
 * Created on May 4, 2008, 3:26:13 PM
 *
 * $Id$
 */
package org.mitre.ccv.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mitre.ccv.CompleteCompositionVector;
import org.mitre.ccv.CompositionDistribution;
import org.mitre.ccv.CompositionVector;
import weka.core.matrix.Matrix;

/**
 * A {@link CompleteCompositionVector} class that uses an {@link IndexedCompositionDistribution} to back the vector.
 * 
 * @author Marc Colosimo
 */
public class IndexedCompleteCompositionVector
        implements CompleteCompositionVector {

    private static final Log LOG = LogFactory.getLog(
                IndexedCompleteCompositionVector.class);

    /** List of our CompositionVectors */
    private List<IndexedCompositionVector> cvs;
    private String name = "";
    private final Integer seqId;
    private final int start;
    private final int stop;
    
    /** Backing CompositionDistribution */
    private IndexedCompositionDistribution compDist;

    /**
     * Construct a new <tt>IndexedCompleteCompositionVector</tt> object
     * using the given sequence composition distributions.
     * 
     * @param seqName
     * @param seq
     * @param begin
     * @param end
     * @param index
     */
    public IndexedCompleteCompositionVector(String seqName, Integer seqId, 
            int start, int stop,
            IndexedCompositionDistribution cd) {

        /** TODO: add value checking */
        this.start = start;
        this.stop = stop;
        this.name = seqName;
        this.seqId = seqId;
        this.compDist = cd;

        this.cvs = new LinkedList<IndexedCompositionVector>();
        LOG.debug(String.format( "Calculating CompositionVectors for %s", seqName) );
        for (int i = this.start; i <= this.stop; i++) {
            this.cvs.add(new IndexedCompositionVector(i, cd));
        }
    }
    
    /**
     * Construct a new <tt>IndexedCompleteCompositionVector</tt> object
     * using the calculated compositions stored in the
     * given composition distribution.
     * 
     * @param seqName Name of sequence
     * @param cd CompositionDistribution to use.
     */
    public IndexedCompleteCompositionVector(String seqName, Integer seqId,
            IndexedCompositionDistribution cd) {
        this.start = cd.startingWindowSize();
        this.stop = cd.endingWindowSize();
        this.name = seqName;
        this.seqId = seqId;
        this.compDist = cd;
        
        this.cvs = new LinkedList<IndexedCompositionVector>();
        for (int i = this.start; i <= this.stop; i++) {
            cvs.add(new IndexedCompositionVector(i, cd));
        }   
    }

    /**
     * Adds this sequence to the given composition distributions.
     * 
     * This <B>DOES NOT</B> capture the boundary cases between the old and new sequences.
     *
     * @param distribution TreeMap of CompositionDistribution to add this ccv to
     */
    public void sumDistribution(CompositionDistribution cd) {
       // cd.addSequence(this.sequence);
        throw new UnsupportedOperationException("Not supported!");
    }

    /**
     * Returns all the non-zero nmers over the range used to generate the 
     * complete composition vector
     * 
     * @return TreeSet of all of the nmers.
     */
    public TreeSet<String> getNmerSet() {
        TreeSet<String> nmers = new TreeSet<String>();
        Iterator<IndexedCompositionVector> cvIter = cvs.iterator();
        while (cvIter.hasNext()) {
            IndexedCompositionVector v = cvIter.next();
            nmers.addAll(v.getNmers());
        }
        return nmers;
    }

    /**
     * Returns an size(nmers) by 1 matrix with pi-values.
     *
     * @param nmers
     */
    public Matrix getMatrix(Set<String> n) {
        // should be abstracted?
        ArrayList<String> nmers = new ArrayList<String>(n);
        int count = nmers.size();
        Matrix ccv = new Matrix(count, 1);

        for (int i = 0; i < count; i++) {
            Double value = this.getPiValueforNmer(nmers.get(i));
            if (value == null) {
                value = 0.0;
            }
            ccv.set(i, 0, value);
        }

        return ccv;
    }

    /**
     * Returns the pi-value for the n-mer (tile).
     */
    public Double getPiValueforNmer(String nmer) {
        int length = nmer.length();
        return cvs.get(length - start).getPiValueForNmer(nmer);
    }

    /**
     * Returns the name of the underlining sequence.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns a unqiue identifier for the sequence that this
     * <tt>CompleteCompositionVector</tt> represents.
     */
    public Integer getSequenceId() {
        return this.seqId;
    }
        
    /**
     * Returns the CompositionDistribution used to generate the vectors.
     */
    public CompositionDistribution getCompositionDistribution() {
        return this.compDist;
    }

    /**
     * Returns starting window size.
     */
    public int getStart() {
        return start;
    }

    /**
     * Returns ending window size.
     */
    public int getStop() {
        return stop;
    }

    /**
     * Returns the <code>CompositionVector</code> for the given windowSize.
     */
    public CompositionVector getCompositionVector(Integer windowSize) {
        return this.cvs.get(windowSize - this.start);
    }
}

/**
 * CompleteCompositionVector.java
 * 
 * Created 9 Jan 2008
 * 
 * $Id$
 */
package org.mitre.ccv;

import java.util.Set;
import java.util.TreeSet;
import weka.core.matrix.Matrix;

/**
 * Interface representing the complete composition vector over a 
 * given range of window sizes. Implementing classes should be backed
 * by a <tt>CompositionVector</tt>.
 * 
 * @author Marc Colosimo
 */
public interface CompleteCompositionVector {

    /**
     * Returns the name of the sequence, which might not be unqiue
     */
    public String getName();

    /**
     * Returns a unqiue identifier for the sequence that this
     * <tt>CompleteCompositionVector</tt> represents.
     */
    public Integer getSequenceId();

    /**
     * Returns starting window size.
     */
    public int getStart();

    /**
     * Returns ending window size.
     */
    public int getStop();

    /**
     * Returns the CompositionVector for the given window size.
     */
    public CompositionVector getCompositionVector(Integer windowSize);

    /**
     * Returns an size(nmers) by 1 matrix with pi-values.
     *
     * @param nmers
     */
    public Matrix getMatrix(Set<String> nmers);

    /**
     * Returns all the non-zero nmers over the range used to generate the
     * complete composition vector
     *
     * @return TreeSet of all of the nmers.
     */
    public TreeSet<String> getNmerSet();

    /**
     * Returns the pi-value for the given n-mer.
     */
    public Double getPiValueforNmer(String nmer);

    /**
     * Returns the CompositionDistribution used to generate the vectors.
     */
    public CompositionDistribution getCompositionDistribution();
}
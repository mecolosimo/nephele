/**
 * VectorSet.java
 * 
 * Created on 5 Jan 2008
 * 
 * $Id$
 */

package org.mitre.ccv;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.mitre.bio.phylo.DistanceMatrix;


/**
 * Interface for a collection (list) of CompleteCompositionVectors
 * calculated over a given range of window sizes.
 * 
 * @author Marc Colosimo
 */
public interface VectorSet {
   /**
     * Add the sequence to this set of vectors.
     * 
     * @param seqName
     * @param seq
     */
    public void addSequence(String seqName, String seq);

    /**
     * Returns the list of CompleteCompositionVectors
     */
    public List<CompleteCompositionVector> getVectors();

    /** 
     * Returns the starting window size.
     */
    public Integer getStart();

    /**
     * Returns the ending window size.
     */
    public Integer getStop();

    /**
     * Returns the list of nmers found in at least one sequence
     * @return java.util.TreeSet
     */
    public TreeSet<String> getNmers() ;

    /**
     * Returns a list sample names in order processed
     */
    public ArrayList<String> getSampleNames();
        
    /**
     * Find the m top nmers with the highest entropy
     * (<code>null</code> or zero is allowed and will return all nmers).
     * 
     * @param m
     * @return java.util.TreeSet of top m nmers found
     */
    public TreeSet<String> getNmers(Integer m) ;
    

    /**
     * Returns a Matrix of the non-zero nmers by sequence
     * (<code>null</code> or empty Set is allowed and this 
     * will return a matrix with all the nmers).
     */
    public CompleteMatrix getFullMatrix(TreeSet<String> nmers) ;
    
    /**
     * Returns a Matrix of the non-zero nmers by sequence
     * (<code>null</code> or zero is allowed and this 
     * will return a matrix with all the nmers).
     * 
     * @return weka.core.matrix.Matrix
     */
    public CompleteMatrix getFullMatrix(Integer topNmers) ;
    
    /**
     * Returns matrix of the non-zero nmers by sequence
     * @param topNmers
     * @param entfile
     * @return
     */
    public CompleteMatrix getFullMatrix(Integer topNmers, String entfile) ;
    
    /**
     * Return a matrix of the pi-values for each nmer using all of
     * the sequences. This basically returns a matrix for a super 
     * sequence. This does not guarantee that it will generate a 
     * concated sequence which will include the nmers overlapping
     * the sequences.
     * 
     * @param nmers the set of nmers to generate a matrix for
     */
    public CompleteMatrix getCompleteMatrix(Set<String> nmers) ;
    
    /**
     * Creates a distance Matrix from the sequence set using the given nmers.
     * 
     * @param matrix weka.core.matrix.Matrix
     * @return {@link DistanceMatrix} representing the distances
     */
    public DistanceMatrix createEuclidianDistanceMatrix(CompleteMatrix completeMatrix) ;
    
    /**
     * Creates Manhattan Matrix from the sequence set using the given nmers
     * @param matrix weka.core.matrix.Matrix
     * @return {@link DistanceMatrix} representing the distances
     */
    public DistanceMatrix createMHDistanceMatrix(CompleteMatrix completeMatrix) ;

    /**
     * Creates a squared Euclidian distance matrix
     * 
     * @param matrix weka.core.matrix.Matrix
     * @return {@link DistanceMatrix} representing the distances
     */
    public DistanceMatrix createESDistanceMatrix(CompleteMatrix completeMatrix) ;

    /**
     * Creates a cosine distance matrix
     * 
     * @param weka.core.matrix.Matrix
     * @return {@link DistanceMatrix} representing the distances
     */
    public DistanceMatrix createCosineDistanceMatrix(CompleteMatrix completeMatrix);

    /**
     * Creates a Jaccard distance matrix.
     * 
     *@param an optional list (set) of nmers to calculate from (intersection).
     *       If <code>null</code> or empty then it will use all nmers it finds.
     *
     * @return {@link DistanceMatrix} representing the distances
     */
    public DistanceMatrix createJaccardDistanceMatrix(ArrayList<String> nmerList);
}

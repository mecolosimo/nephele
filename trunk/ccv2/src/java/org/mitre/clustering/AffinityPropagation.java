/**
 * $Id$
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
 */
package org.mitre.clustering;

import weka.core.matrix.Matrix;
import java.util.Vector;
import java.util.Iterator;

/**
 * A class that implements “affinity propagation,” which takes as input measures
 * of similarity between pairs of data points. Real-valued messages are
 * exchanged between data points until a high-quality set of exemplars and
 * corresponding clusters gradually emerges.
 * 
 * Based on Brendan J. Frey and Delbert Dueck, University of Toronto
 * "Clustering by Passing Messages Between Data Points". Science 315, 972–976.
 *
 * @see http://www.psi.toronto.edu/affinitypropagation/
 * @author Matt Peterson
 */
public class AffinityPropagation {
    
    public Matrix s = null;
    public final int count;
    
    public int maxits;
    public int convits;
    public double lam;
    public Boolean nonoise = false;
    
    public Boolean symmetric = true;
    public Boolean unconverged = true;
    
    public double K;
    public Matrix A;
    public Matrix R;
    
    public Vector<Integer> exemplars = null;
    
    
    /**
     * Constructor
     * @param sims, the similarity matrix
     */
    public AffinityPropagation(Matrix sims, double p) {
        this(sims, 5000, 200, 0.90, p);
    }
        
    public AffinityPropagation(Matrix sims, int max, int cons, 
            double lambda, double p) {
        
        s = sims;
        maxits = max;
        convits = cons;
        lam = lambda;
        
        count = s.getColumnDimension();
        
        /*
         * Add noise to get rid of degeneracies
         */
        Matrix rand = Matrix.random(count, count);
        Matrix small = new Matrix(count, count, .0000001);
        
        Matrix t = s.times(Double.MIN_VALUE).plus(small);
        t = t.times(rand);
        s = s.plus(t);
        
        
        /*
         * Put preferences on diagonal of S
         */
        for (int i = 0; i < count; i++) {
            s.set(i, i, p);
        }
        
        try {
            run();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
    }
    

    
    public void run() throws Exception {
        /*
         * Allocate space for messages
         */
        Matrix ds;
        try {
            ds = getDiag(s);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        A = new Matrix(count, count,0.0);
        R = new Matrix(count, count, 0.0);
        int t = 1;
        
        /*
         * Run parralel affinity prop. updates
         */
        Matrix e = new Matrix(count, convits, 0);
        Boolean dn = false;
        int i = -1; // adjusted due to difference in MATLAB indexing
        Matrix st;
        if (!symmetric) {
            st = s;
        } else {st = s.transpose();}
        
        while (!dn) {
            i = i + 1;
            
            A = A.transpose(); R = R.transpose();
            /*
             * Compute responsibilities
             */
            for (int ii = 0; ii < count; ii++) {
                Matrix old = R.getMatrix(0, count-1, ii, ii);
                Matrix AS = A.getMatrix(0,count-1, ii,ii).plus(
                        st.getMatrix(0, count-1, ii, ii));
                
                int I = getMaxIndex(AS);
                double Y = AS.get(I, 0);
                /*
                 * Need to use -Double.MAX_VALUE here, as this will return
                 * the smallest NEGATIVE number, as opposed to smallest absolute
                 * floating point number
                 */
                AS.set(I, 0, -Double.MAX_VALUE);
                
                int I2 = getMaxIndex(AS);
                double Y2 = AS.get(I2, 0);
                
                /*
                 * Set R
                 */
                Matrix yMat = new Matrix(count, 1, Y);
                Matrix stVect = st.getMatrix(0, count-1, ii, ii);
                R.setMatrix(0, count-1, ii,ii, stVect.minus(yMat));
                R.set(I, ii, st.get(I, ii) - Y2);
                Matrix rVect = R.getMatrix(0,count-1, ii, ii);
                rVect = rVect.times(1-lam).plus(old.times(lam));
                R.setMatrix(0, count-1, ii, ii, rVect);
            }
            A = A.transpose(); R = R.transpose();
            
            /*
             * Compute availabilities
             */
            for (int jj = 0; jj < count; jj++) {
                Matrix old = A.getMatrix(0, count-1, jj, jj);
                Matrix Rp = new Matrix(count, 1, 0);
                for (int c = 0; c < count; c++) {
                    if (R.get(c, jj) < 0.0) {
                        Rp.set(c,0,0.0);
                    } else {
                        Rp.set(c,0 , R.get(c,jj));
                    }
                }
                
                Rp.set(jj, 0, R.get(jj, jj));
                Matrix sumM = new Matrix(count, 1, getSum(Rp));
                A.setMatrix(0, count-1, jj, jj, sumM.minus(Rp));
                
                double dA = A.get(jj,jj);
                
                Matrix aVect = A.getMatrix(0,count-1,jj,jj);
                for(int c = 0; c < count; c++) {
                    if (aVect.get(c, 0) > 0 ) {
                        aVect.set(c,0,0.0);
                    }
                }
                A.setMatrix(0, count-1, jj, jj, aVect);
                A.set(jj,jj, dA);
                
                
                aVect = A.getMatrix(0,count-1,jj,jj);
                aVect = aVect.times(1-lam).plus(old.times(lam));
                A.setMatrix(0,count-1, jj, jj, aVect);
                
            }
            
            /*
             * Check for convergence
             */
            Matrix E = null;
            try {
                E = getDiag(A).plus(getDiag(R));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            
            for (int c = 0; c < count; c++) {
                if ( E.get(c, 0) > 0 ) {
                    E.set(c, 0, 1.0);
                } else {
                    E.set(c, 0, 0.0);
                }
            }
            
            int index = i % convits;
            
            e.setMatrix(0, count-1, index, index, E);
            
            K = getSum(E);
            
            if (i >= convits || i >= maxits) {
                Matrix se = new Matrix(count, 1);
                for (int c = 0; c < count; c++) {
                    double sum = 0;
                    for (int c2 = 0; c2 < convits; c2++) {
                        sum += e.get(c, c2);
                    }
                    se.set(c, 0, sum);
                }
                
                Matrix seC = matrixIsEqual(se, convits);
                Matrix seZ = matrixIsEqual(se, 0.0);
                
                double value = getSum(seC.plus(seZ));
                
                if (value == count) {
                    unconverged = false;
                } else {
                    unconverged = true;
                }
                
                if ( (!unconverged && K>0) || i==maxits) {
                    dn = true;
                }

            }
            
        }
        
        /*
         * Identify exemplars
         */
        Matrix diag = null;
        try {
            diag = getDiag(A).plus(getDiag(R));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
        exemplars = new Vector<Integer>();
        
        for (int c = 0; c < count; c++) {
            if (diag.get(c,0) > 0) {
                exemplars.add(c);
            }
        }        
        
    }
    
    public int[] getClusters() {
        int[] clusters = new int[count];
        double[] values = new double[count];
        for (int c = 0; c < count; c++) {
            values[c] = -Double.MAX_VALUE;
        }
        
        
        /*
         * Get clusters
         */
        Iterator<Integer> it = exemplars.iterator();
        
        int excount = exemplars.size();
        
        for(int c = 0; c < count; c++) {
            for (int c2 = 0; c2 < excount; c2++) {
                double value = s.get(c, exemplars.get(c2));
                if (value > values[c]) {
                    values[c] = value;
                    clusters[c] = c2;
                }
            }
        }
        
       /*
        * Refine clusters
        */
        for (int c = 0; c< excount; c++) {
            clusters[exemplars.get(c)] = c;
        }
        
        
        return clusters;
    }
    
    /**
     * Function to estimate machine epsilon
     * 
     * Input parameters: None
     * Output parameters: eps, an estimation of machine epsilon
     */
    private double calcEps() {
        double eps = 1.0e0;
        double epsp1 = 0.0;
        
        do {
            eps = eps/2.0e0;
            epsp1 = 1.0e0 + eps;
        } while (epsp1 > 1.0e0);
        
        return eps;
    }
    
    /**
     * Returns the diagonal of a square matrix
     * 
     * @param m
     * @return
     */
    private Matrix getDiag(Matrix m) throws Exception {
        
        if (m.getRowDimension() != m.getColumnDimension()) {
            throw new Exception("Matrix must be square");
        }
        
        int c = m.getRowDimension();
        
        Matrix rm = new Matrix(c, 1, 0);
        
        for (int i = 0; i < c ; i++ ) {
            rm.set(i, 0, m.get(i, i));
        }
        
        return rm;
    }
    
    private int getMaxIndex(Matrix m) {
        
        double max = m.get(0,0);
        int index = 0;
            
        for (int i = 0; i < m.getRowDimension(); ++i) {
            double value = m.get(i, 0);
            if (value > max) {
                index = i;
                max = value;
            }
        }
        
        return index;
    }
    
    private double getSum(Matrix m) { 
        double [] vector = m.getColumnPackedCopy();
        double sum = 0;
        
        for (int i = 0; i < vector.length; i++) {
            sum += vector[i];
        }
        
        return sum;
    }
    
    private Matrix matrixIsEqual(Matrix m, double v) {
        Matrix r = new Matrix(count, 1);
        
        for (int c = 0; c < count; c++) {
            if (m.get(c, 0) == v) {
                r.set(c, 0, 1.0);
            }
        }
        
        return r;
    }
   
}

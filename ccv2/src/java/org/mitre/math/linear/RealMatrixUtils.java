/**
 * Created on 9 Sept 2009
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
package org.mitre.math.linear;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.math.linear.Array2DRowRealMatrix;
import org.apache.commons.math.linear.BlockRealMatrix;
import org.apache.commons.math.linear.MatrixUtils;
import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.linear.SingularValueDecompositionImpl;

//import org.mitre.bio.phylo.DistanceMatrix;

/**
 * Some Weka/JAMA like matrix functions missing from Apache's commons math package
 *
 * @author Marc Colosimo
 */
public class RealMatrixUtils {
    private static final Log LOG = LogFactory.getLog("RealMatrixUtils");

    private static final RealMatrixUtils singleton;

    static {
        singleton = new RealMatrixUtils();
    }

    /**
     * Checks for "use.block.real.matrix" System property and if set (to anything), this will return
     * {@link BlockRealMatrix} instead of the default {@link Array2DRowRealMatrix}.
     *
     * @param rowDimension
     * @param columnDimension
     * @return a RealMatrix
     */
    static public RealMatrix getNewRealMatrix(int rowDimension, int columnDimension) {
        // @todo: fix code in phylogeny-core
        /**
        if (System.getProperty("use.block.real.matrix") == null) {
            return new Array2DRowRealMatrix(rowDimension, columnDimension);
        } else {
            return new BlockRealMatrix(rowDimension, columnDimension);
        }*/
        return MatrixUtils.createRealMatrix(rowDimension, columnDimension);
    }

    static public RealMatrix getNewRealMatrix(int rowDimension, int columnDimension, double initialValue) {
        RealMatrix matrix;
        /**
        if (System.getProperty("use.block.real.matrix") == null) {
            matrix = new Array2DRowRealMatrix(rowDimension, columnDimension);
        } else {
            matrix = new BlockRealMatrix(rowDimension, columnDimension);
        }*/

        matrix = MatrixUtils.createRealMatrix(rowDimension, columnDimension);
        for (int i = 0; i < rowDimension; i++) {
            for (int j = 0; j < columnDimension; j++) {
                matrix.setEntry(i, j, initialValue);
            }
        }

        return matrix;
    }

    /**
     * Return a singlete instances of this class (no public constructor)
     */
    public  static RealMatrixUtils getSingleton() {
        return singleton;
    }

    private RealMatrixUtils() { } // Private constructor

    /**
     * Element-by-element multiplication, C = A.*B
     * @param B    another matrix of same size
     * @return     A.*B
     */
    public RealMatrix arrayTimes(RealMatrix A, RealMatrix B) {
        checkMatrixDimensions(A, B);
        int m = A.getRowDimension();
        int n = A.getColumnDimension();
        RealMatrix C = getNewRealMatrix(m, n);
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                C.setEntry(i, j, A.getEntry(i, j) * B.getEntry(i, j));
            }
        }
        return C;
    }

    /**
     * Multiply a matrix by a scalar, C = s*A
     * @param s    scalar
     * @return     s*A
     */
    public RealMatrix times(RealMatrix A, double s) {
        int m = A.getRowDimension();
        int n = A.getColumnDimension();
        RealMatrix C = getNewRealMatrix(m, n);
        //double[][] C = X.getArray();
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                C.multiplyEntry(i, j, s);
                //C[i][j] = s*A[i][j];
            }
        }
        return C;
    }

    /**
     * Multiply a matrix by a scalar in place, A = s*A
     * @param s    scalar
     * @return     replace A by s*A
     */
    public void timesEquals(RealMatrix A, double s) {
        int m = A.getRowDimension();
        int n = A.getColumnDimension();
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                //A[i][j] = s*A[i][j];
                A.multiplyEntry(i, j, s);
            }
        }
    }

    /**
     * One norm returns the <a href="http://mathworld.wolfram.com/MaximumAbsoluteColumnSumNorm.html">
     * maximum absolute column sum norm </a> of the matrix
     *
     * @return norm1
     */
    public double norm1(RealMatrix matrix) {
        double f = 0;
        for (int j = 0; j < matrix.getColumnDimension(); j++) {
            double s = 0;
            for (int i = 0; i < matrix.getRowDimension(); i++) {
                s += Math.abs(matrix.getEntry(i, j));
            }
            f = Math.max(f, s);
        }
        return f;
    }

    /**
     * Two norm returns the <a href="http://mathworld.wolfram.com/L2-Norm.html"> L2-Norm </a>
     * @return    maximum singular value.
     */
    public double norm2(RealMatrix matrix) {
        return (new SingularValueDecompositionImpl(matrix).getNorm());
    }

    /**
     * Normalizes a matrix into its Z-scores.
     */
    public void normalizeMatrix(RealMatrix matrix) {
        //int features = matrix.getRowDimension();
        //int samples = matrix.getColumnDimension();
        int m = matrix.getRowDimension();
        int n = matrix.getColumnDimension();

        // Normalize each row
        for (int i = 0; i < m; i++) {
            // would be easier/quicker if we can get the whole row as an array (or vector)
            RealMatrix subMatrix = matrix.getSubMatrix(i, i, 0, n - 1);     // n - 1 for 0 based indexing
            double sum = norm1(subMatrix) / n;
            // minusEquals (subtractEquals) subMatrix
            for (int j = 0; j < n; j++) {
                subMatrix.addToEntry(0, j, -1.0 * sum);
            }
            double std = norm1(arrayTimes(subMatrix, subMatrix)) / n;
            timesEquals(subMatrix, 1.0 / std);
            
            //setSubMatrix(i,i, 0, samples -1, m)
            for (int j = 0; j < n; j++) {
                matrix.setEntry(i, j, subMatrix.getEntry(0, j));
            }
        }

        // Normalize each feature
        //for (int i = 0; i < features; i++) {
        //Matrix m = matrix.getMatrix(i, i, 0, samples - 1);
        //double sum = m.norm1() / samples;
        //double sum = norm1(m) / samples;
        //m.minusEquals(new Matrix(1, samples, sum));
        //double std = m.arrayTimes(m).norm1() / samples;
        //m.times(1.0 / std);
        //matrix.setMatrix(i, i, 0, samples - 1, m);

        //}
    }

    /**
     * Check if size(A) == size(B)
     */
    private void checkMatrixDimensions(RealMatrix A, RealMatrix B) {
        if (A.getRowDimension() != B.getRowDimension() || B.getColumnDimension() != A.getColumnDimension()) {
            throw new IllegalArgumentException("Matrix dimensions must agree.");
        }
    }
}

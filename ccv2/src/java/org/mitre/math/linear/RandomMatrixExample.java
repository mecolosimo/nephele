/**
 *
 * Created on 16 Sept 2009.
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

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;

import java.util.Locale;
import org.apache.commons.math.linear.BlockRealMatrix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.MathRuntimeException;
import org.apache.commons.math.linear.Array2DRowRealMatrix;
import org.apache.commons.math.linear.DefaultRealMatrixChangingVisitor;
import org.apache.commons.math.linear.DefaultRealMatrixPreservingVisitor;
import org.apache.commons.math.linear.RealMatrix;

/**
 *
 * @author Marc Colosimo
 */
public class RandomMatrixExample {

    private static final Log LOG = LogFactory.getLog(RandomMatrixExample.class);

    public class RandomRealMatrixChangingVisitor extends DefaultRealMatrixChangingVisitor {

        @Override
        public double visit(int row, int column, double value) {
            if (value == 0.0) {
                return Math.random();
            } else {
                LOG.debug("matrix has non-zero intial value");
                return Math.random();
            }
        }
    }

    public class PopulateMatrixChangingVisitor extends DefaultRealMatrixChangingVisitor {

        BlockRealMatrix m;

        public PopulateMatrixChangingVisitor(BlockRealMatrix matrix) {
            m = matrix;
        }

        @Override
        public double visit(int row, int column, double value) {
            //if (row == 0)
            //    System.err.printf("0,%d=%f\t", column, m.getEntry(row, column));
            return m.getEntry(row, column);
        }
    }

    public class ComparingPreservingVisitor extends DefaultRealMatrixPreservingVisitor {

        RealMatrix m;

        public ComparingPreservingVisitor(RealMatrix matrix) {
            m = matrix;
        }

        @Override
        public void visit(int row, int column, double value) {
            // @TODO: NO WAY TO BREAK OUT!
            // need ways to compare to a decimal place
              if (m.getEntry(row, column) != value) 
                    throw new MathRuntimeException("({0}x{1}): A ({2}) and B ({3}) are not the same!\n", 
                            row, column, Double.toString(m.getEntry(row, column)), Double.toString(value));
        }
    }

    /**
     * Generate random variables for the given matrix.
     *
     * @param M a matrix to fill with random variables
     */
    public double[][] randomMatrixVariables(int m, int n) {
        double[][] M = new double[m][n];
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                M[i][j] = Math.random();
            }
        }
        return M;
    }

    public boolean compareMatrices(RealMatrix A, RealMatrix B) {
        for (int i = 0; i < A.getRowDimension(); i++) {
            //System.out.printf("Checking row %d\n", i);
            for (int j = 0; j < A.getColumnDimension(); j++) {
                if (  A.getEntry(i, j) !=   B.getEntry(i, j)) {
                    LOG.warn(String.format("(%d,%d): A (%f) and B (%f) are not the same!\n",
                            i, j, A.getEntry(i, j), B.getEntry(i, j)));
                    return false;
                }
            }
        }
        return true;
    }

    public String localTmpFile() {
        long now = System.currentTimeMillis();
        return "RandomMatrixExample_" + Long.toHexString(now);
    }

    public int generateRandomMatrix(final String path, final int rows, final int columns) throws IOException {
        File file = new File(this.localTmpFile());
        BufferRealMatrix t = new BufferRealMatrix(rows, columns, file);
        t.walkInOptimizedOrder(new RandomRealMatrixChangingVisitor());
        return 0;
    }

    public boolean compareMatrices(final String matrixPath1, final String matrixPath2) throws IOException {
        System.err.println("Unimplemented!");
        return true;
    }

    /**
     * 
     * @param m
     * @param width      Column width.
     * @param digits     Number of digits after the decimal.
     * @return
     */
    public String matrix2String(RealMatrix m, int width, int digits) {
        DecimalFormat format = new DecimalFormat();
        format.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.US));
        format.setMinimumIntegerDigits(1);
        format.setMaximumFractionDigits(digits);
        format.setMinimumFractionDigits(digits);
        format.setGroupingUsed(false);

        StringBuilder output = new StringBuilder();
        output.append('\n');  // start on new line.
        for (int i = 0; i < m.getRowDimension(); i++) {
            for (int j = 0; j < m.getColumnDimension(); j++) {
                String s = format.format(m.getEntry(i, j)); // format the number
                int padding = Math.max(1, width - s.length()); // At _least_ 1 space
                for (int k = 0; k < padding; k++) {
                    output.append(' ');
                }
                output.append(s);
            }
            output.append('\n');
        }
        output.append('\n');   // end with blank line.
        return output.toString();
    }

    public int run(String args[]) throws Exception {

        int m = 100;
        int n = 60;
        boolean generate = false;

        ArrayList<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    m = Integer.parseInt(args[++i]);
                } else if ("-c".equals(args[i])) {
                    n = Integer.parseInt(args[++i]);
                } else if ("-g".equals(args[i])) {
                    generate = true;
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException ex) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return -1;
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i - 1]);
                return -1;
            }

        }

        if (generate) {
            LOG.info(String.format("Generating random %dx%d matrix '%s'", m, n, other_args.get(0)));
            return this.generateRandomMatrix(other_args.get(1), m, n);
        }

        
        LOG.info(String.format("Making random matrix %d}x%d", m, n));
        double[][] M = this.randomMatrixVariables(m, n);

        LOG.info(String.format("Creating Array2DRowRealMatrix from random matrix %dx%d", m, n));
        Array2DRowRealMatrix arm = new Array2DRowRealMatrix(M);

        LOG.info(String.format("Creating BlockRealMatrix from random matrix %dx%d", m, n));
        BlockRealMatrix brm = new BlockRealMatrix(M);

        LOG.info(String.format("Checking Array2DRow and BlockRealMatrix"));
        if (!this.compareMatrices(arm, brm)) {
            LOG.fatal("One of these are not like the other!");
        }

        LOG.info(String.format("Creating BufferRealMatrix %dx%d", m, n));
        BufferRealMatrix mbrm = new BufferRealMatrix(m, n, null);

        LOG.info(String.format("Populating BufferRealMatrix %dx%d from random matrix", m, n));
        //mbrm.walkInOptimizedOrder(new PopulateMatrixChangingVisitor(brm));
        /**/
        // not the speedest use walkInOptimizedOrder!
           for (int i = 0; i < brm.getRowDimension(); i++) {
            for (int j = 0; j < brm.getColumnDimension(); j++) {
                mbrm.setEntry(i, j, brm.getEntry(i, j));
            }
           }
         //  System.err.println(this.matrix2String(mbrm, n, 8));

        LOG.info("Checking block matrix verse buffer matrix...");
        mbrm.walkInOptimizedOrder(new ComparingPreservingVisitor(brm));

        LOG.info("Checking matrix addition...");
        BlockRealMatrix brm2 = brm.add(new BlockRealMatrix(M));
        BufferRealMatrix mbrm_tmp = new BufferRealMatrix(m, n, null);
        mbrm_tmp.walkInOptimizedOrder(new PopulateMatrixChangingVisitor(brm));
        BufferRealMatrix mbrm2 = mbrm.add(mbrm_tmp);
        mbrm2.walkInOptimizedOrder(new ComparingPreservingVisitor(brm2));

        LOG.info("Checking matrix multiplication (none are numerically stable to the final decimal places)...");
        BlockRealMatrix brm_tmp = new BlockRealMatrix(n, m);
        brm_tmp.walkInOptimizedOrder(new RandomRealMatrixChangingVisitor() );
        LOG.info("Starting BlockRealMatrix Multiplication");
        brm2 = brm.multiply(brm_tmp);
        LOG.info("Finished BlockRealMatrix Multiplication");
        
        mbrm_tmp = new BufferRealMatrix(n,m, null);
        mbrm_tmp.walkInOptimizedOrder(new PopulateMatrixChangingVisitor(brm_tmp));
        LOG.info("Starting BufferRealMatrix Multiplication");
        mbrm2 = mbrm.multiply(mbrm_tmp);
        LOG.info("Finished BufferRealMatrix Multiplication");
        LOG.info("Checking BlockRealMatrix to BufferRealMatrix");
        mbrm2.walkInOptimizedOrder(new ComparingPreservingVisitor(brm2));

        final int bufferBlockLength = BufferRealMatrix.BLOCK_SIZE;
        int startRow = m / 2;
        int endRow = m - 1;
        int startColumn = n / 2;
        int endColumn = n - 1;
        LOG.debug(String.format("Checking getting buffer submatrix (%d,%d) to (%d,%d)", startRow, startColumn, endRow, endColumn));
        double[][] sub = mbrm.getSubMatrixData(startRow, endRow, startColumn, endColumn);
        LOG.info(String.format("Found %f at submatrix 0,0 and %f for %d,%d", sub[0][0], mbrm.getEntry(startRow, startColumn), startRow, startColumn));

        LOG.info("Allocation testing, doubling size each iteration");
        int am = m;
        int an = n;
        LOG.info("Starting allocating BlockRealMatrices....");
        try {
            while (true) {
                am *= 2;
                an *= 2;
                BlockRealMatrix t = new BlockRealMatrix(am, an);
            }
        } catch (OutOfMemoryError e) {
            LOG.info(String.format("Ran out of memory generating %dx%d matrix", am, an));
        }

        LOG.info("Creating BufferRealMatrix at last attempted in size...");
        File file = new File(this.localTmpFile());
        BufferRealMatrix t = new BufferRealMatrix(am, an, file);

        // slow way need to optimize for blocks
        t.walkInOptimizedOrder(new RandomRealMatrixChangingVisitor());

        LOG.info(String.format("(0,2) = %f", t.getEntry(0, 2)));

        t = null;
        LOG.info("Attempting to reload matrix");
        BufferRealMatrix rt = BufferRealMatrix.loadMatrix(file);
        LOG.info(String.format("reloaded (0,2) = %f", rt.getEntry(0, 2)));

        return 0;
    }

    /**
     * main() has some simple utility methods
     */
    public static void main(String argv[]) throws Exception {
        RandomMatrixExample rme = new RandomMatrixExample();
        System.exit(rme.run(argv));
    }
}

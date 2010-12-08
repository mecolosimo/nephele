/**
 * Created on 11 Sept 2009.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 * 
 * $Id$
 */
package org.mitre.math.linear;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.math.MathRuntimeException;
import org.apache.commons.math.linear.AbstractRealMatrix;
import org.apache.commons.math.linear.BlockRealMatrix;
import org.apache.commons.math.linear.MatrixIndexException;
import org.apache.commons.math.linear.MatrixUtils;
import org.apache.commons.math.linear.MatrixVisitorException;
import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.linear.RealMatrixChangingVisitor;
import org.apache.commons.math.linear.RealMatrixPreservingVisitor;

/**
 * A {@link DoubleBuffer} RealMatrix class that supports storing
 * the matrix to disk allowing for much larger matrices.
 *
 * This uses a 64 MB block size which is a block of 1024 by 1024 of doubles.
 * 
 * @see BlockRealMatrix
 * @author Marc Colosimo
 */
public class BufferRealMatrix extends AbstractRealMatrix implements Serializable {

    private static final Log LOG = LogFactory.getLog(BufferRealMatrix.class);
    public final static String TEMP_FILE_PREFIX = "buffer_real_matrix";
    /** Size of a Double in bytes. */
    public static final int DOUBLE_BYTE_SIZE = Double.SIZE / Byte.SIZE;
    /** Size of an Integer in bytes. */
    public static final int INTEGER_BYTE_SIZE = Integer.SIZE / Byte.SIZE;
    /** Block size in bytes, defaut is a 64 MB block. */
    public static final int BLOCK_BYTE_SIZE = 67108864;
    /** SQRT of BLOCK_BYTE_SIZE, length of one side of a square in bytes. */
    public static final int BLOCK_BYTE_LENGTH = 8192;
    /** Length of a size of a block, 1024 doubles */
    public static final int BLOCK_SIZE = BLOCK_BYTE_LENGTH / DOUBLE_BYTE_SIZE;
    /** Size of our header in bytes: block size, row, column. */
    public static final int BUFFER_HEADER_SIZE = 3 * INTEGER_BYTE_SIZE;
    /** Underlining file channel containing data. */
    private final FileChannel dataFileChannel;
    /** Number of rows of the matrix. */
    private final int rows;
    /** Number of columns of the matrix. */
    private final int columns;
    /** Number of block rows of the matrix. */
    private final int blockRows;
    /** Number of block columns of the matrix. */
    private final int blockColumns;

    /** Used for retrieving and storing values: getEntry/setEntry */
    private final ByteBuffer entryDoubleByteBuffer = ByteBuffer.allocate(DOUBLE_BYTE_SIZE);

    private BufferRealMatrix(final FileChannel fileChannel, final int rows, final int columns) throws IllegalArgumentException {
        super(rows, columns);
        this.rows = rows;
        this.columns = columns;

        // @todo: should we check to see if fileChannel header matches what was given?

        // number of blocks
        this.blockRows = (rows + BLOCK_SIZE - 1) / BLOCK_SIZE;
        this.blockColumns = (columns + BLOCK_SIZE - 1) / BLOCK_SIZE;

        this.dataFileChannel = fileChannel;
    }

    /**
     * Create a new matrix with the supplied row and column dimensions.
     *
     * @param rows      the number of rows in the new matrix
     * @param columns   the number of columns in the new matrix
     * @param file      the file to use to store the mapped matrix (<code>null</code> allowed and a tempFile will be created)
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public BufferRealMatrix(final int rows, final int columns, File file) throws IllegalArgumentException, IOException {
        super(rows, columns);
        this.rows = rows;
        this.columns = columns;

        // number of blocks
        this.blockRows = (rows + BLOCK_SIZE - 1) / BLOCK_SIZE;
        this.blockColumns = (columns + BLOCK_SIZE - 1) / BLOCK_SIZE;

        if (file == null) {
            file = File.createTempFile(TEMP_FILE_PREFIX, null);
            LOG.debug(String.format("Created tempFile '%s'", file.getAbsolutePath()));
        }
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        this.dataFileChannel = raf.getChannel();

        long mbbSize = (long) (Double.SIZE / Byte.SIZE) * (long) rows * (long) columns + BUFFER_HEADER_SIZE;
        LOG.debug(String.format("Matrix size will be %d bytes and %d by %d blocks", mbbSize, this.blockRows, this.blockColumns));

        MappedByteBuffer bb = this.dataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_HEADER_SIZE);
        bb.clear();
        bb.putInt(BLOCK_BYTE_SIZE);
        bb.putInt(rows);
        bb.putInt(columns);
    // note: we don't create the layout like BlockedRealMatrix
    // Is this set to zeros? It would be a pain/slow to init it if it is realy big
    }

    public static BufferRealMatrix loadMatrix(File file) throws FileNotFoundException, IOException {
        return loadMatrix(new RandomAccessFile(file, "rw").getChannel());
    }

    /**
     * Attempts to load in a previously saved matrix from the given {@link FileChannel}
     *
     * @param fileChannel
     * @return
     * @throws IOException
     */
    public static BufferRealMatrix loadMatrix(FileChannel fileChannel) throws IOException {
        MappedByteBuffer bb = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_HEADER_SIZE);
        bb.clear();

        int block_size = bb.getInt();
        assert (block_size == BLOCK_BYTE_SIZE);

        int rows = bb.getInt();
        int columns = bb.getInt();
        LOG.debug(String.format("Found matrix of %dx%d with %d block sizes", rows, columns, block_size));
        return new BufferRealMatrix(fileChannel, rows, columns);
    }

    /**
     * creates a matrix using the supplied channel
     * 
     * <P>
     * @param rowDimension
     * @param columnDimension
     * @param fileChannel
     * @return
     * @throws IllegalArgumentException
     */
    public static BufferRealMatrix createMatrix(int rowDimension, int columnDimension, FileChannel fileChannel) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /** {@inheritDoc} */
    @Override
    public BufferRealMatrix createMatrix(int rowDimension, int columnDimension) throws IllegalArgumentException {
        try {
            return new BufferRealMatrix(rowDimension, columnDimension, null);
        } catch (IOException iex) {
            throw new IllegalArgumentException(iex);
        }
    }

    @Override
    public RealMatrix copy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Compute the sum of this and <code>m</code>.
     *
     * @param m    matrix to be added
     * @return     this + m
     * @throws  IllegalArgumentException if m is not the same size as this
     */
    public BufferRealMatrix add(final BufferRealMatrix b)
            throws IllegalArgumentException {

        // safety checks
        if (b == this) {
            return this.addSelf();
        }
        MatrixUtils.checkAdditionCompatible(this, b);

        try {
            final BufferRealMatrix c = new BufferRealMatrix(rows, columns, null);

            // perform addition block-wise, to ensure good cache behavior
            for (int blockIndex = 0; blockIndex < this.blockRows * this.blockColumns; ++blockIndex) {
                // all the same size, so should all be the same blockOffsets and layout
                final long blockOffset = this.getBlockOffset(blockIndex);
                DoubleBuffer adb = this.dataFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                adb.clear();
                DoubleBuffer bdb = b.dataFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                bdb.clear();
                DoubleBuffer cdb = c.dataFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                cdb.clear();
                for (int k = 0; k < BLOCK_BYTE_SIZE / DOUBLE_BYTE_SIZE; k++) {
                    try {
                        cdb.put(adb.get() + bdb.get());
                    } catch (BufferUnderflowException e) {
                        LOG.fatal(String.format("BufferUnderflowException while adding elements at %d in block %d", k, blockIndex));
                        throw e;
                    }
                }
            }
            return c;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /** Special case for adding to ourself */
    private BufferRealMatrix addSelf() {
        try {
            final BufferRealMatrix c = new BufferRealMatrix(rows, columns, null);

            // perform addition block-wise, to ensure good cache behavior
            for (int blockIndex = 0; blockIndex < this.blockRows * this.blockColumns; ++blockIndex) {
                // all the same size, so should all be the same blockOffsets and layout
                final long blockOffset = this.getBlockOffset(blockIndex);
                DoubleBuffer adb = this.dataFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                adb.clear();
                DoubleBuffer cdb = c.dataFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                cdb.clear();
                for (int k = 0; k < BLOCK_BYTE_SIZE / DOUBLE_BYTE_SIZE; k++) {
                    try {
                        double ad = adb.get();
                        cdb.put(ad + ad);
                    } catch (BufferUnderflowException e) {
                        LOG.fatal(String.format("BufferUnderflowException while adding elements at %d in block %d", k, blockIndex));
                        throw e;
                    }
                }
            }
            return c;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /** {@inheritDoc} */
    public BufferRealMatrix subtract(final BufferRealMatrix b) throws IllegalArgumentException {

        //        if (b == this) {
        //    return this.subtractSelf();
       // }
        // safety check
        MatrixUtils.checkSubtractionCompatible(this, b);

        try {
            final BufferRealMatrix c = new BufferRealMatrix(rows, columns, null);

            // perform addition block-wise, to ensure good cache behavior
            for (int blockIndex = 0; blockIndex < this.blockRows * this.blockColumns; ++blockIndex) {
                // all the same size, so should all be the same blockOffsets and layout
                final long blockOffset = this.getBlockOffset(blockIndex);
                DoubleBuffer adb = this.dataFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                adb.clear();
                DoubleBuffer bdb = b.dataFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                bdb.clear();
                DoubleBuffer cdb = c.dataFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                cdb.clear();
                for (int k = 0; k < BLOCK_BYTE_SIZE / DOUBLE_BYTE_SIZE; k++) {
                    try {
                        cdb.put(adb.get() - bdb.get());
                    } catch (BufferUnderflowException e) {
                        LOG.fatal(String.format("BufferUnderflowException while adding elements at %d in block %d", k, blockIndex));
                        throw e;
                    }
                }
            }
            return c;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /** {@inheritDoc} */
    public BufferRealMatrix scalarAdd(final double d) {
        try {
            final BufferRealMatrix c = new BufferRealMatrix(rows, columns, null);

            // perform addition block-wise, to ensure good cache behavior
            for (int blockIndex = 0; blockIndex < this.blockRows * this.blockColumns; ++blockIndex) {
                // all the same size, so should all be the same blockOffsets and layout
                final long blockOffset = this.getBlockOffset(blockIndex);
                DoubleBuffer adb = this.dataFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                adb.clear();
                DoubleBuffer cdb = c.dataFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                cdb.clear();
                for (int k = 0; k < BLOCK_BYTE_SIZE / DOUBLE_BYTE_SIZE; k++) {
                    try {
                        double ad = adb.get();
                        cdb.put(ad + d);
                    } catch (BufferUnderflowException e) {
                        LOG.fatal(String.format("BufferUnderflowException while adding elements at %d in block %d", k, blockIndex));
                        throw e;
                    }
                }
            }
        return c;
        } catch (IllegalArgumentException ex) {
           LOG.fatal(ex);
           throw new RuntimeException(ex);
        } catch (IOException ex) {
            LOG.fatal(ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * Returns the result of postmultiplying this by m.
     *
     * @param m    matrix to postmultiply by
     * @return     this * m
     * @throws     IllegalArgumentException
     *             if columnDimension(this) != rowDimension(m)
     */
    public BufferRealMatrix multiply(final BufferRealMatrix b) throws IllegalArgumentException {

        // safety check
        MatrixUtils.checkMultiplicationCompatible(this, b);

        try {
            final BufferRealMatrix c = new BufferRealMatrix(rows, b.columns, null);
            // allocate one row for our matrix
            final ByteBuffer abb = ByteBuffer.allocate(BLOCK_SIZE * DOUBLE_BYTE_SIZE);
            // for some funny reason we can't get an array, even if we wrap it before! So, allocate it here and use latter
           //  final double[] ar = new double[BLOCK_SIZE]; This isn't faster

            // perform multiplication block-wise, to ensure good cache behavior
            int blockIndex = 0;
            for (int iBlock = 0; iBlock < c.blockRows; ++iBlock) {
                final int pStart = iBlock * BLOCK_SIZE;
                final int pEnd = Math.min(pStart + BLOCK_SIZE, rows);
                //System.err.printf("pStart=%d\tpEnd=%d\tblockRows=%d\tblockColumns=%d\n", pStart, pEnd, c.blockRows, c.blockColumns);
                for (int jBlock = 0; jBlock < c.blockColumns; ++jBlock) {
                    final int jWidth = BLOCK_SIZE;             // square block no matter what
                    final int jWidth2 = jWidth + jWidth;
                    final int jWidth3 = jWidth2 + jWidth;
                    final int jWidth4 = jWidth3 + jWidth;

                    // select current product block
                    DoubleBuffer cdb = c.dataFileChannel.map(
                            FileChannel.MapMode.READ_WRITE,
                            c.getBlockOffset(blockIndex), BLOCK_BYTE_SIZE).asDoubleBuffer();
                    cdb.clear();

                    // perform multiplication on current block
                    for (int kBlock = 0; kBlock < blockColumns; ++kBlock) {
                        //final int kWidth = blockWidth(kBlock);
                        final int kWidth = BLOCK_SIZE;

                        LOG.debug(String.format("Getting a block %d and b block %d", iBlock * blockColumns + kBlock, kBlock * b.blockColumns + jBlock));
                        
                        // walk down the blocks columns
                        DoubleBuffer bdb = b.dataFileChannel.map(
                                FileChannel.MapMode.READ_WRITE,
                                b.getBlockOffset(kBlock * b.blockColumns + jBlock), BLOCK_BYTE_SIZE).asDoubleBuffer();
                        bdb.clear();

                        LOG.debug("Processing blocks");
                        for (int p = pStart, k = 0; p < pEnd; ++p) {
                            // a's width (# cols) is the same as b's height (# rows) and c's width
                            final int lStart = (p - pStart) * kWidth;           // Square padded with zeros    
                            final int lEnd = blockWidth(kBlock);                // Can stop at the last column in a's block
                            //System.err.printf("k=%d\tp=%d\tlstart=%d\tlend=%d\t\n", k, p, lStart, lEnd);
                            // For each row in a, multiple the columns in b
                            // Can stop at the last column in the c's block which should be the last column in b

                            // walk across A's blocks rows grabbing a row at a time
                            abb.clear();
                            this.dataFileChannel.position(this.getBlockOffset(iBlock * blockColumns + kBlock) + (lStart * DOUBLE_BYTE_SIZE));
                            final int r = this.dataFileChannel.read(abb);  // relative get into local bytebuffer
                            //System.err.printf("Got %d bytes (%d doubles) for %d block width\n", r, r / DOUBLE_BYTE_SIZE, kWidth);
                            if ( r == -1) {
                                LOG.fatal("Unable to read in data");
                            }
                            abb.clear();
                            final DoubleBuffer adb = abb.asDoubleBuffer();
                            adb.clear();
                            // tried getting access to local copy (array) but it wasn't faster access

                            for (int nStart = 0; nStart < c.blockWidth(jBlock); ++nStart) {
                                double sum = 0;
                                int l = 0;  // first column in this row
                                int n = nStart;
                                // do four at a time (why four?)
                                adb.position(l);
                                
                                while (l < lEnd - 3) {
                                    sum += adb.get() * bdb.get(n) +
                                           adb.get() * bdb.get(n + jWidth) +
                                           adb.get() * bdb.get(n + jWidth2) +
                                            adb.get() * bdb.get(n + jWidth3);
                                    l += 4;
                                    n += jWidth4;
                                }
                                while (l < lEnd) {
                                    sum += adb.get() * bdb.get(n);
                                    n += jWidth;
                                    l++;
                                }
                                sum += cdb.get(k);
                                cdb.put(k++, sum);
                                //System.err.printf("k=%d\tn=%d\n", k, n);
                            }
                            // correct k for difference in blockWidth since we are always square
                            k = (p + 1) * BLOCK_SIZE;
                            //System.err.printf("end of p-loop (%d), k=%d\n", p, k);
                         }
                   }
                    this.dataFileChannel.force(false);
                    System.err.printf("Finished block %d\n", blockIndex);
                    // go to next block
                    ++blockIndex;
                }
            }
            return c;
        } catch (IOException ex) {
            throw new IllegalArgumentException(ex.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override
    public double getEntry(final int row, final int column) throws MatrixIndexException {
        try {
            
            this.dataFileChannel.position( this.getEntryBlockOffset(row, column) + (this.getEntryIndex(row, column) * DOUBLE_BYTE_SIZE));
            this.entryDoubleByteBuffer.clear(); // makes a buffer ready for a new sequence of channel-read or relative put operations
            final int r = this.dataFileChannel.read(this.entryDoubleByteBuffer);  // relative get into local bytebuffer
            if ( r == -1) {
                throw new MatrixIndexException(
                    "Error at reading value at index ({0}, {1}), double byte offset %d",
                    row, column, this.getEntryBlockOffset(row, column) + (this.getEntryIndex(row, column) * DOUBLE_BYTE_SIZE));
            }
            //System.err.printf("Got %f (%d,%d) at buffer index %d (read %d bytes)\n", this.entryDoubleByteBuffer.getDouble(0), row, column, this.getEntryBlockOffset(row, column) + (this.getEntryIndex(row, column) * DOUBLE_BYTE_SIZE), r);
            return this.entryDoubleByteBuffer.getDouble(0);
        } catch (IOException ioe) {
            throw new MatrixIndexException(
                    "IO error getting value at index ({0}, {1}) in a {2}x{3] matrix",
                    row, column, getRowDimension(), getColumnDimension());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setEntry(int row, int column, double value) throws MatrixIndexException {
        try {
            this.entryDoubleByteBuffer.clear(); // makes a buffer ready for a new sequence of channel-read or relative put operations
            this.entryDoubleByteBuffer.putDouble(value);  // don't use absolute put only relative put!
            this.entryDoubleByteBuffer.flip(); // makes a buffer ready for a new sequence of channel-write or relative get operations
            this.dataFileChannel.position( this.getEntryBlockOffset(row, column) + (this.getEntryIndex(row, column) * DOUBLE_BYTE_SIZE));
            final int w = this.dataFileChannel.write(this.entryDoubleByteBuffer); // w = bytes wrote
            //System.err.printf("Put %f (%d,%d) at buffer index %d (wrote %d) (ByteBuffer[0]=%f)\n", value, row, column, (this.getEntryBlockOffset(row, column) + this.getEntryIndex(row, column)) * DOUBLE_BYTE_SIZE, w, this.entryDoubleByteBuffer.getDouble(0));
         } catch (IOException ioe) {
            throw new MatrixIndexException(
                    "IO error at getting block with indices ({0}, {1}), double byte offset %d",
                    row, column, this.getEntryBlockOffset(row, column) + (this.getEntryIndex(row, column) * DOUBLE_BYTE_SIZE));
        }
    }

    /** {@inheritDoc} */
    @Override
    public void addToEntry(final int row, final int column, final double increment) throws MatrixIndexException {
        double value = this.getEntry(row, column) + increment;
        this.setEntry(row, column, value);
    }

    /** {@inheritDoc} */
    @Override
    public void multiplyEntry(final int row, final int column, final double factor) throws MatrixIndexException {
        double value = this.getEntry(row, column) * factor;
        this.setEntry(row, column, value);
    }

    /** {@inheritDoc} */
    @Override
    public int getRowDimension() {
        return this.rows;
    }

    /** {@inheritDoc} */
    @Override
    public int getColumnDimension() {
        return this.columns;
    }

    /**
     * Gets a submatrix. Rows and columns are indicated
     * counting from 0 to n-1.
     *
     * @param startRow Initial row index
     * @param endRow Final row index (inclusive)
     * @param startColumn Initial column index
     * @param endColumn Final column index (inclusive)
     * @return The subMatrix containing the data of the
     *         specified rows and columns
     * @exception MatrixIndexException  if the indices are not valid
     */
    public double[][] getSubMatrixData(final int startRow, final int endRow,
            final int startColumn, final int endColumn)
            throws MatrixIndexException {

        // safety checks
        MatrixUtils.checkSubMatrixIndex(this, startRow, endRow, startColumn, endColumn);

        // create the output matrix
        final double[][] out = new double[endRow - startRow + 1][endColumn - startColumn + 1];

        // compute blocks shifts
        final int blockStartRow = startRow / BLOCK_SIZE;
        final int blockEndRow = endRow / BLOCK_SIZE;
        final int blockStartColumn = startColumn / BLOCK_SIZE;
        final int blockEndColumn = endColumn / BLOCK_SIZE;

        // perform extraction block-wise, to ensure good cache behavior
        for (int iBlock = blockStartRow; iBlock <= blockEndRow; ++iBlock) {
            for (int jBlock = blockStartColumn; jBlock <= blockEndColumn; ++jBlock) {
                // blocks are rows major order
                final int blockIndex = iBlock * this.blockColumns + jBlock;
                try {
                    final long blockOffset = this.getBlockOffset(blockIndex);
                    final DoubleBuffer block = this.dataFileChannel.map(
                            FileChannel.MapMode.READ_WRITE,
                            blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                    block.clear();
                    final int iStart = Math.max(startRow, iBlock * BLOCK_SIZE);
                    final int iEnd = Math.min(endRow, (iBlock * BLOCK_SIZE) + BLOCK_SIZE);
                    final int jStart = Math.max(startColumn, jBlock * BLOCK_SIZE);
                    final int jEnd = Math.min(endColumn, (jBlock * BLOCK_SIZE) + BLOCK_SIZE);

                    LOG.info(String.format("blockIndex=%d (%d, %d), iStart=%d, iEnd=%d, jStart=%d, jEnd=%d", blockIndex, jBlock, iBlock, iStart, iEnd, jStart, jEnd));
                    for (int i = iStart; i < iEnd; ++i) {
                        for (int j = jStart; j < jEnd; ++j) {
                            // data in blocks are rows major, get our index in the block
                            final int index = (i - iBlock * BLOCK_SIZE) * BLOCK_SIZE +
                                    (j - jBlock * BLOCK_SIZE);
                            out[i - startRow][j - startColumn] = block.get(index);
                        }
                    }
                } catch (IOException ioe) {
                    throw new MathRuntimeException("IO Exception while visiting blockIndex {0} (iBlock={1}, jBlock={2})",
                            blockIndex, iBlock, jBlock);
                }
            }
        }
        return out;
    }

    /** {@inheritDoc} */
    @Override
    public double walkInOptimizedOrder(final RealMatrixChangingVisitor visitor)
            throws MatrixVisitorException {
        visitor.start(rows, columns, 0, rows - 1, 0, columns - 1);
        for (int iBlock = 0, blockIndex = 0; iBlock < blockRows; ++iBlock) {
            final int pStart = iBlock * BLOCK_SIZE;
            final int pEnd = Math.min(pStart + BLOCK_SIZE, rows);

            for (int jBlock = 0; jBlock < blockColumns; ++jBlock, ++blockIndex) {
                try {
                    final int qStart = jBlock * BLOCK_SIZE;
                    final int qEnd = Math.min(qStart + BLOCK_SIZE, columns);

                    final long blockOffset = this.getBlockOffset(blockIndex);
                    LOG.debug(String.format("BlockIndex=%d (offset=%d) pStart=%d pEnd=%d qStart=%d qEnd=%d", blockIndex, blockOffset, pStart, pEnd, qStart, qEnd));

                    final DoubleBuffer block = this.dataFileChannel.map(
                            FileChannel.MapMode.READ_WRITE,
                            blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                    block.clear();

                    for (int p = pStart, k = 0; p < pEnd; ++p) {
                        // jump to end of row incase we are not there
                        k = (p - pStart) * BLOCK_SIZE;
                        for (int q = qStart; q < qEnd; ++q, ++k) {
                            block.put(k, visitor.visit(p, q, block.get(k)));
                        }
                    }
                    this.dataFileChannel.force(false);
                } catch (IOException ioe) {
                    throw new MathRuntimeException("IO Exception while visiting blockIndex {0} (iBlock={1}, jBlock={2})",
                            blockIndex, iBlock, jBlock);
                }
            }
        }
        return visitor.end();
    }

    /** {@inheritDoc} */
    @Override
    public double walkInOptimizedOrder(final RealMatrixPreservingVisitor visitor)
            throws MatrixVisitorException {
        visitor.start(rows, columns, 0, rows - 1, 0, columns - 1);
        for (int iBlock = 0, blockIndex = 0; iBlock < blockRows; ++iBlock) {
            final int pStart = iBlock * BLOCK_SIZE;
            final int pEnd = Math.min(pStart + BLOCK_SIZE, rows);

            for (int jBlock = 0; jBlock < blockColumns; ++jBlock, ++blockIndex) {
                try {
                    final int qStart = jBlock * BLOCK_SIZE;
                    final int qEnd = Math.min(qStart + BLOCK_SIZE, columns);

                    final long blockOffset = this.getBlockOffset(blockIndex);
                    final DoubleBuffer block = this.dataFileChannel.map(
                            FileChannel.MapMode.READ_WRITE,
                            blockOffset, BLOCK_BYTE_SIZE).asDoubleBuffer();
                    block.clear();

                    LOG.debug(String.format("BlockIndex=%d pStart=%d pEnd=%d qStart=%d qEnd=%d", blockIndex, pStart, pEnd, qStart, qEnd));
                    for (int p = pStart, k = 0; p < pEnd; ++p) {
                        // jump to end of row incase we are not there
                        k = (p - pStart) * BLOCK_SIZE;
                        for (int q = qStart; q < qEnd; ++q, ++k) {
                            visitor.visit(p, q, block.get(k));
                        }
                    }

                    this.dataFileChannel.force(false);
                } catch (IOException ioe) {
                    throw new MathRuntimeException("IO Exception while visiting blockIndex {0} (iBlock={1}, jBlock={2})",
                            blockIndex, iBlock, jBlock);
                }
            }
        }
        return visitor.end();
    }

    /**
     * Returns the index in a block of the row,column.
     *
     * @param row
     * @param column
     */
    private int getEntryIndex(int row, int column) {
        final int iBlock = row / BLOCK_SIZE;
        final int jBlock = column / BLOCK_SIZE;
        // data in blocks are rows major, get our index in the block
        return (row - iBlock * BLOCK_SIZE) * BLOCK_SIZE + (column - jBlock * BLOCK_SIZE);
    }

    /**
     * Get the block offset containing the given row, column.
     *
     * @param row
     * @param column
     * @return
     */
    private long getEntryBlockOffset(int row, int column) {
        final long iBlock = row / BLOCK_SIZE;
        final long jBlock = column / BLOCK_SIZE;
        // blocks are rows major, get our block index and then jump the required number of doubles per blocks
        final long i = (iBlock * this.blockColumns + jBlock) * BLOCK_BYTE_SIZE;
        return BUFFER_HEADER_SIZE + i;
    }

    /**
     * Get the offset in the buffer of the given block
     */
    private long getBlockOffset(int block) {
        return BUFFER_HEADER_SIZE + block * (long) BLOCK_BYTE_SIZE;
    }

    /**
     * Get the height of a block.
     * @param blockRow row index (in block sense) of the block
     * @return height (number of rows) of the block
     */
    private int blockHeight(final int blockRow) {
        return (blockRow == blockRows - 1) ? rows - blockRow * BLOCK_SIZE : BLOCK_SIZE;
    }

    /**
     * Get the width of a block.
     * @param blockColumn column index (in block sense) of the block
     * @return width (number of columns) of the block
     */
    private int blockWidth(final int blockColumn) {
        return (blockColumn == blockColumns - 1) ? columns - blockColumn * BLOCK_SIZE : BLOCK_SIZE;
    }
}

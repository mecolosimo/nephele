/**
 * Created on July 2, 2009.
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
package org.mitre.nio;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Random;

/**
 * A lexicographically unsigned byte comparator that is cabilibly of comparing {@link ByteBuffer}s using a random shuffle.
 *
 * @author Marc Colosimo
 */
public class UnsignedByteBufferShuffleComparator implements Comparator<ByteBuffer> {

    private long seed;
    /** How many bytes to shuffle at a time. Default is per byte, a value of 4 would be an int. */
    private int stride = 1;

    private int[] permutedList = null;

    public UnsignedByteBufferShuffleComparator(long initialSeed) {
        this.seed = initialSeed;
    }

    /**
     * Constructor for a new comparator.
     * 
     * @param initialSeed
     * @param byteStride
     */
    public UnsignedByteBufferShuffleComparator(long initialSeed, int byteStride) {
        this.seed = initialSeed;
        this.stride = (byteStride < 1) ? 1:byteStride;
    }

    /**
     * Two byte buffers are compared by comparing their randomly shuffled sequences of remaining elements lexicographically, without regard to the starting position of each sequence within its corresponding buffer.
     *
     * A byte buffer is not comparable to any other type of object.
     *
     * @return A negative integer, zero, or a positive integer as this buffer is less than, equal to, or greater than the given buffer.
     */
    public int compare(ByteBuffer b1, ByteBuffer b2) {

        int pos1 = b1.position();
        int pos2 = b2.position();

        int val = shuffleCompare(b1, b2);
      
        // jump back so that we can do this again (using this as a key or the like).
        b1.position(pos1);
        b2.position(pos2);
        return val;

    }

    /**
     * 
     * 
     * @param b1
     * @param b2
     * @return
     */
    private int shuffleCompare(ByteBuffer b1, ByteBuffer b2) {
        //System.err.printf("shuffleStrideComparing with stride of %d\n", this.stride);
        //if (b1.hasArray() && b2.hasArray()) {
        //    return shuffleCompare(b1.array(), b1.arrayOffset() + b1.position(),
        //            b2.array(), b2.arrayOffset() + b2.position());
        //}

        final int num = Math.min(b1.remaining(), b2.remaining());
        final int num_strides = num/this.stride;

        Random rgen = new Random(this.seed);
        final int[] pList = this.getPermutedList(rgen, num_strides);

        /**
         * pick an element from a random position (r_pos), only picking something
         * at or above our current position (count) at stride boundaries.
         */
        final int pos_this = b1.position();
        final int pos_other = b2.position();
        for (int i = 0; i < num_strides; i++) {
            final int r_pos = pList[i] * this.stride;
            //System.err.printf("stride=%d\tnum=%d\tr_pos=%d\n", i, num, r_pos);

            /** Compare the stride bytes */
            for (int idx = 0; idx + r_pos < num; idx++) {
                final int a = b1.get(pos_this + r_pos + idx ) & 0xFF;
                final int b = b2.get(pos_other + r_pos + idx) & 0xFF;

                if (a == b) {
                    continue;
                }
                if (a < b) {
                    return -1;
                }                
                return 1;
            }
        }

        // if zero, then the same
        return b1.remaining() - b2.remaining();
    }

    private int shuffleCompare(byte[] b1, int pos1, byte[] b2, int pos2) {
        final int num = Math.min(b1.length - pos1, b2.length - pos2);
        final int num_strides = num/this.stride;

        final int[] pList = this.getPermutedList(new Random(this.seed), num_strides);

        /**
         * pick an element from a random position (r_pos), only picking something
         * at or above our current position (count) at stride boundaries.
         */
        for (int i = 0; i < num_strides; i++) {
            final int r_pos = pList[i] * this.stride;

            /** Compare the stride bytes */
            for (int idx = 0; idx + r_pos < num; idx++) {
                final int a = b1[pos1 + r_pos + idx] & 0xFF;
                final int b = b2[pos2 + r_pos + idx] & 0xFF;

                if (a == b) {
                    continue;
                }

                if (a < b) {
                    return -1;
                }

                return 1;
            }
        }

        return (b1.length - num) - (b2.length - num);
    }

    /**
     * Return or generate a shuffled/permuted list of positions
     *
     * @param rgen
     * @param num_strides
     */
    private int[] getPermutedList(Random rgen, int num_strides) {
        if (this.permutedList == null || this.permutedList.length != num_strides) {
            /** make a new permuted list of positions */
            //this.permutedList = IntBuffer.allocate(num_strides);
            this.permutedList = new int[num_strides];
            for (int i = 0; i < num_strides; i++) {
                this.permutedList[i] = i;
            }

            /** shuffle the list */
            for (int i = 0; i < num_strides; i++) {
                int j = rgen.nextInt(num_strides - i) + i;
                /** if selection is not i, then we need to swap  */
                if (j != i) {
                    int t = this.permutedList[i];
                    //System.err.printf("Swapping %d for %d\n", t, j);
                    this.permutedList[i] = j; //.put(i, j);
                    this.permutedList[j] = t; //.put(j, t);
                }
            }
            //this.permutedList.rewind();
        }
        return this.permutedList;
    }
}

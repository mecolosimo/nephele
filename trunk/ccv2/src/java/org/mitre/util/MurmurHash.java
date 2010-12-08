/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * $Id$
 */
package org.mitre.util;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup.  See http://murmurhash.googlepages.com/ for more details.
 *
 * <p>The C version of MurmurHash 2.0 found at that site was ported
 * to Java by Andrzej Bialecki (ab at getopt org).</p>
 *
 * <p>hash64A was ported by Marc Colosimo (mcolosimo at mitre.org) from the C version
 * found at http://en.wikipedia.org/wiki/MurmurHash.</p>
 */
public class MurmurHash {

    private static MurmurHash singleton = new MurmurHash();

    public static MurmurHash getSingleton() {
        return singleton;
    }

    public int hash(String data, int seed) {
        byte[] bytes = data.getBytes();
        return hash(bytes, bytes.length, seed);
    }

    /**
     * 32-bit hash
     * @param data
     * @param length
     * @param seed
     * @return
     */
    public int hash(byte[] data, int length, int seed) {
        int m = 0x5bd1e995;
        int r = 24;

        int h = seed ^ length;

        int len_4 = length >> 2;

        for (int i = 0; i < len_4; i++) {
            int i_4 = i << 2;
            int k = data[i_4 + 3];
            k = k << 8;
            k = k | (data[i_4 + 2] & 0xff);
            k = k << 8;
            k = k | (data[i_4 + 1] & 0xff);
            k = k << 8;
            k = k | (data[i_4 + 0] & 0xff);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // avoid calculating modulo
        int len_m = len_4 << 2;
        int left = length - len_m;

        if (left != 0) {
            if (left >= 3) {
                h ^= data[length - 3] << 16;
            }
            if (left >= 2) {
                h ^= data[length - 2] << 8;
            }
            if (left >= 1) {
                h ^= data[length - 1];
            }

            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    /**
     * 64-bit hash (MurmurHash64A).
     *
     * <p>This is assumes the machine (VM) is 64-bit native, but will work on 32-bit machines.</p>
     *
     * @param data
     * @param length
     * @param seed
     * @return a long hash of the data. Note that Java only has signed longs
     */
    public long hash64A(byte[] data, int length, int seed) {
        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        long h = seed ^ length;

        // Mix 8 bytes at a time into the hash
        int len_8 = length >> 3;
        for (int i = 0; i < len_8; i++) {
            int i_8 = i << 3;
            long k = unsigned64ByteArrayToLong(data, i_8);

            k *= m;
            k ^= k >> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        // handle the remaining bytes
        int idx_8 = len_8 << 3;
        switch (length & 7) {
            case 7:
                h ^= ((long) data[idx_8 + 6]) << 48;
            case 6:
                h ^= ((long) data[idx_8 + 5]) << 40;
            case 5:
                h ^= ((long) data[idx_8 + 4]) << 32;
            case 4:
                h ^= ((long) data[idx_8 + 3]) << 24;
            case 3:
                h ^= ((long) data[idx_8 + 2]) << 16;
            case 2:
                h ^= ((long) data[idx_8 + 1]) << 8;
            case 1:
                h ^= ((long) data[idx_8]);
                h *= m;
        }

        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }

    /**
     * Convert the byte array containing 64-bits to a long starting from
     * the given offset.
     *
     * @param b The byte array
     * @param offset The array offset
     * @return The integer
     */
    public static long unsigned64ByteArrayToLong(byte[] b, int offset) {
        int value = 0;
        for (int i = 0; i < 8; i++) {
            int shift = (8 - 1 - i) * 8;
            value ^= ((long) b[i + offset] & 0xFF) << shift;
        }
        return value;
    }
}
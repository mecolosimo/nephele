/**
 * Created on June 4, 2009.
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
package org.mitre.util;

import java.util.List;

/**
 * A class that hashes of a set of items (array of bytes) and returns the minimum over these hash values.
 *
 * <p>This is not to be confused with Minwise Independent Permutation Hashing. 
 * However, it should work for Locality Sensitive Hashing (LSH).<p>
 *
 * <p>Only works well for datasets with fewer than 2**32 - 1(~4 billion)
 * unique items in it. Larger than that and, due to use our hash
 * function using singed 64-bit integers, we run into the birthday
 * paradox.<p>
 *
 * @author Marc Colosimo
 */
public class MinHash {

    private static MinHash singleton = new MinHash();

    public static MinHash getSingleton() {
        return singleton;
    }

    public int minhash(List<byte[]> data, int seed) {
        MurmurHash mhash = MurmurHash.getSingleton();
        int val = Integer.MAX_VALUE;
        for (byte[] bytes : data) {
            int n = mhash.hash(bytes, bytes.length, seed);
            val = (n < val) ? n : val;
        }
        return val;
    }

    /**
     * Supports upto 2^32 - 1 unique items (has a smaller chance of collision).
     *
     * @param data
     * @param seed
     * @return the min hash64 of from the data.
     */
    public long minhash64(List<byte[]> data, int seed) {
        MurmurHash mhash = MurmurHash.getSingleton();
        long val = Long.MAX_VALUE;
        for (byte[] bytes : data) {
            long n = mhash.hash64A(bytes, bytes.length, seed);
            val = (n < val) ? n : val;
        }
        return val;
    }
}

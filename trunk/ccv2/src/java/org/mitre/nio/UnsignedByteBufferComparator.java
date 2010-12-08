/**
 * Created on June 30, 2009.
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

/**
 * A {@link Comparator} class that can be used to lexicographically sort two {@link ByteBuffer}s as unsigned bytes.
 *
 * This resets each buffer to the point (position) they had before being compared.
 *
 * @author Marc Colosimo
 */
public class UnsignedByteBufferComparator implements Comparator<ByteBuffer> {

    /**
     * Two byte buffers are compared by comparing their sequences of remaining elements lexicographically, without regard to the starting position of each sequence within its corresponding buffer.
     *
     * A byte buffer is not comparable to any other type of object.
     *
     * @return A negative integer, zero, or a positive integer as this buffer is less than, equal to, or greater than the given buffer.
     */
    public int compare(ByteBuffer b1, ByteBuffer b2) {

        
        int pos1 = b1.position();
        int pos2 = b2.position();

        //if (b1.hasArray() && b2.hasArray()) {
            // should be faster and JVM could optimize this
            //return ByteBufferUtils.compareUnsignedTo(b1.array(), b1.arrayOffset() + pos1, b2.array(), b2.arrayOffset() + pos2);
        //}

        int val = ByteBufferUtils.compareUnsignedTo(b1, b2);

        // jump back so that we can do this again (using this as a key or like).
        b1.position(pos1);
        b2.position(pos2);
        return val;
    }
}

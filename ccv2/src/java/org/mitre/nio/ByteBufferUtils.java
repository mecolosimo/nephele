/**
 * Created on June 29, 2009.
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
import java.util.ArrayList;

/**
 *
 * @author Marc Colosimo
 */
public class ByteBufferUtils {

    /**
     * Allocates a new ByteBuffer with a capacity of the sum of the <b>{@link ByteBuffer#limit()}</b> 
     * of the two given ByteBuffers and cancatinates them together.
     * If a limit is not set, then {@link ByteBuffer#capacity()} will be used. 
     * Also if one buffer is directly allocated, then the returning buffer will be directly allocated.
     * 
     * @param b1
     * @param b2
     * @return
     */
    public static ByteBuffer concat(ByteBuffer b1, ByteBuffer b2) {
        int capacity = ((b1.limit() == 0 ? b1.capacity() : b1.limit())) + ((b2.limit() == 0 ? b2.capacity() : b2.limit()));
        ByteBuffer dst;
        if (b1.isDirect() || b2.isDirect()) {
            dst = ByteBuffer.allocateDirect(capacity);
        } else {
            dst = ByteBuffer.allocate(capacity);
        }
        b1.rewind();
        dst.put(b1);
        b2.rewind();
        dst.put(b2);
        return dst;
    }

    /**
     * Encodes a byte buffer into hex (no spaces) string from the current position.
     *
     * This preserves the current position of the {@link ByteBuffer}.
     * @param byteBuffer
     * @return
     */
    public static String toHexString(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        int pos = byteBuffer.position();
        //byteBuffer.rewind();
        while (byteBuffer.hasRemaining()) {
            String hex = Integer.toHexString(0xFF & byteBuffer.get());
            if (hex.length() == 1) {
                sb.append("0");
            }
            sb.append(hex);
        }
        byteBuffer.position(pos);
        return sb.toString();
    }

    public static ByteBuffer fromHexString(String decode) {

        String[] ds = decode.split(" ");
        ArrayList<Byte> dbl = new ArrayList<Byte>();
        for (int i = 0; i < ds.length; i++) {
            String hex = ds[i];
            for (int j = 0; j < hex.length(); j += 2) {
                // bytes in Java are signed and bad things occur with Byte.parseByte(hex, 16)
                // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6259307
                byte b = (byte) ( Integer.parseInt(hex.substring(j, j + 2), 16) & 0xFF );
                dbl.add(b);
                //System.err.printf("%s->%d\n", hex.substring(j, j + 2), b);
            }
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(dbl.size());
        for (Byte b : dbl) {
            byteBuffer.put(b);
        }
        byteBuffer.rewind();
        return byteBuffer;
    }

    /**
     * Compares two ByteBuffers as unsigned bytes (normally bytes are signed).
     *
     * Two byte buffers are compared by comparing their sequences of remaining elements lexicographically, without regard to the starting position of each sequence within its corresponding buffer.
     *
     * A byte buffer is not comparable to any other type of object.
     *
     * @return A negative integer, zero, or a positive integer as this buffer is less than, equal to, or greater than the given buffer
     * @param b1
     * @param b2
     */
    public static int compareUnsignedTo(final ByteBuffer b1, final ByteBuffer b2) {

        //final ByteBuffer other = b2;

        final int num = Math.min(b1.remaining(), b2.remaining());
        int pos_this = b1.position();
        int pos_other = b2.position();

        for (int count = 0; count < num; count++) {
            final int a = b1.get(pos_this++) & 0xFF;
            final int b = b2.get(pos_other++) & 0xFF;

            if (a == b) {
                continue;
            }

            if (a < b) {
                return -1;
            }

            return 1;
        }

        return b1.remaining() - b2.remaining();
    }

    /**
     *
     * @param b1 backing array {@link ByteBuffer#array()} of a ByteBuffer(length should equal remaining)
     * @param pos1 starting position, usually {@link ByteBuffer#arrayOffset()} + {@link ByteBuffer#position()}
     * @param b2
     * @param pos2
     * @return
     */
    public static int compareUnsignedTo(final byte[] b1, final int pos1, final byte[] b2, final int pos2) {
        final int num = Math.min(b1.length - pos1, b2.length - pos2);

        for (int count = 0; count < num; count++) {
            final int a = b1[pos1 + count] & 0xFF;
            final int b = b2[pos2 + count] & 0xFF;

            if (a == b) {
                continue;
            }

            if (a < b) {
                return -1;
            }

            return 1;

        }

        return (b1.length - num) - (b2.length - num);
    }
}

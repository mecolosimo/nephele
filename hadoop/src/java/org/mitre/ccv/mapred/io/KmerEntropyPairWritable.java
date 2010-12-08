/**
 * Created on March 30, 2009.
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
package org.mitre.ccv.mapred.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Class for storing k-mer entropy pairs. This compares the entropy (value) first,
 * and if they are equal, then by their k-mer (key).
 * <P>For {@link HashPartitioner} this uses the entropy value {@link KmerEntropyPairWritable#hashCode() }
 * 
 * @author Marc Colosimo
 */
public class KmerEntropyPairWritable implements WritableComparable {

    private String key;
    private Double value;

    public KmerEntropyPairWritable() {
        key = null;
        value = null;
    }

    public KmerEntropyPairWritable(String k, Double v) {
        key = k;
        value = v;
    }

    public void set(String k, Double v) {
        this.value = v;
        this.key = k;
    }

    /**
     * Return the k-mer
     */
    public String getKey() {
        return key;
    }

    public void setKey(String k) {
        this.key = k;
    }

    /**
     * Return the k-mer's value
     */
    public Double getValue() {
        return value;
    }

    public Double addValue(Double ent) {
        value += ent;
        return value;
    }

    public void setValue(Double ent) {
        value = ent;
    }

    /**
     * Clear key and value.
     */
    public void clear() {
        this.key = null;
        this.value = null;
    }

    /**
     * Output as "key\tvalue"
     */
    @Override
    public String toString() {
        return key + "\t" + value;
    }

    // Methods required for Hadoop Partitioners (like the default HashPartioner)
    /** Returns true iff <code>o</code> is a KmerEntropyPairWritable with the same value. */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KmerEntropyPairWritable)) {
            return false;
        }
        return (this.compareTo(o) == 0);
    }

    /**
     * Returns a hash code for this Double object. The result is the exclusive OR of the two halves of the long integer bit representation, exactly as produced by the method Double.doubleToLongBits(double), of the primitive double value represented by this Double object. That is, the hash code is the value of the expression:
     * (int)(v^(v>>>32))
     * <P>
     * where v is defined by:
     * <blockquote>
     * long v = Double.doubleToLongBits(this.doubleValue());
     * <blockquote>
     */
    @Override
    public int hashCode() {
        return value.hashCode();
    }

    // Hadoop WritableComparable methods
    /**
     * Compares the entropy values returning them from highest to lowest (reversed sorting).
     * If they are equal, then it compares the keys (k-mers) returning those in natural lex order.
     */
    @Override
    public int compareTo(Object o) {
        int cmp = Double.compare(this.value, ((KmerEntropyPairWritable) o).value);
        if (cmp != 0) {
            return cmp * -1;
        }
        return key.compareTo(((KmerEntropyPairWritable) o).key);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(0);            // place holder
        out.writeDouble(this.value);
        //out.writeUTF(this.key);
        /**
         * Adapted from Text, but we do not use writeVInt becuase
         * we need to read this back in from a ByteBuffer.
         */
        ByteBuffer bytes = Text.encode(this.key);
        int length = bytes.limit();
        out.writeInt(length);
        out.write(bytes.array(), 0, length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readByte();
        this.value = in.readDouble();
        //this.key = in.readUTF();
        /**
         * Adapted from Text
         */
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        this.key = Text.decode(bytes);
    }

    public void readFields(ByteBuffer in) throws IOException {
        in.get();       // readByte()
        this.value = in.getDouble();
        int length = in.getInt();
        byte[] bytes = new byte[length];
        in.get(bytes, 0, length);
        this.key = Text.decode(bytes);
    }
}

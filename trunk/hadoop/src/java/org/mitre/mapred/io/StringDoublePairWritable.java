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
package org.mitre.mapred.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * A @link{WritableComparable} class for storing <code>String</code>/<code>Double</code> pairs,
 * such as k-mer/pi-value pairs for either local (single sample) or global sequences 
 * {@link CalculateKmerRevisedRelativeEntropy}, sorting on the key.
 * <P>
 * This can store empty <code>Double</code> (pi-values) and defaults to sorting on the <code>String</code> (k-mer) key.
 * <P>
 * This class can also be sorted by either the <code>Double</code> value or <code>String</code> key and value.
 * For the value, the order can be reversed (largest to smallest).
 * 
 * @author Marc Colosimo
 */
public class StringDoublePairWritable implements WritableComparable {

    private String key;
    private Double value;
    private byte flags = 0x00;
    // 'cause math is hard and hex math even harder: http://www.dewassoc.com/support/msdos/decimal_hexadecimal.htm
    static private final byte HAS_VALUE = 0x01;             // 0000 0001
    static private final byte IS_LOCAL = 0x02;              // 0000 0010
    static private final byte COMPARE_VALUE = 0x04;         // 0000 0100
    static private final byte COMPARE_KEY_VALUE = 0x08;     // 0000 1000
    static private final byte REVERSE_KEY_SORT = 0x10;      // 0001 0000
    static private final byte REVERSE_VALUE_SORT = 0x20;    // 0010 0000

    /**
     * Default constructor for an empty local <code>String</code>/<code>Double</code>(k-mer/pi-value) pairing.
     */
    public StringDoublePairWritable() {
        this.clear();
    }

    /**
     * Constructor for <code>String</code>/<code>Double</code>(k-mer/pi-value) pairing.
     * @param k
     * @param v
     * @param local If true, then this is a local (per sample) value.
     *              If <code>false</code>, then treat this as a global.
     */
    public StringDoublePairWritable(String k, Double v, Boolean local) {
        this.clear();
        this.set(k, v, local);
    }

    public StringDoublePairWritable(String k, Double v) {
        this.clear();
        this.set(k, v);
    }

    /**
     * Sets the key and value as local.
     */
    public void set(String k, Double v) {
        this.set(k, v, true);
    }

    /**
     * Sets the key and value.
     * @param local If true, then this is a local (per sample) value.
     *      If <code>false</code>, then treat this as a global.
     *
     */
    public void set(String k, Double v, Boolean local) {
        this.clear();
        this.setKmer(k);
        this.setValue(v);

        if (local) {
            this.setFlag(IS_LOCAL);
        } else {
            this.clearFlag(IS_LOCAL);
        }
    }

    public void setKmer(String k) {
        this.key = k;
    }

    public String getKmer() {
        return this.key;
    }

    public void setValue(Double v) {
        this.value = v;
        if (v == null) {
            this.setFlag(HAS_VALUE);
        } else {
            this.clearFlag(HAS_VALUE);
        }
    }

    public Double getValue() {
        return this.value;
    }

    /**
     * Returns <code>true</code> if this is a local (single sequence) pi-value.
     */
    public boolean isLocal() {
        return this.flagSet(IS_LOCAL);
    }

    /**
     * Returns <code>true</code> if this has a pi-value.
     */
    public boolean hasValue() {
        return this.flagSet(HAS_VALUE);
    }

    /**
     * Use the keys (<code>String</code>) to compare objects (i.e., sorts by the keys). Default compare.
     * @param reverse If <code>true</code>, reverse the natural sort order.
     */
    public void compareKeys(boolean reverse) {
        this.clearFlag(COMPARE_VALUE);
        this.clearFlag(COMPARE_KEY_VALUE);
        if (reverse)  {
            this.setFlag(REVERSE_KEY_SORT);
        } else {
            this.clearFlag(REVERSE_KEY_SORT);
        }
    }

    /**
     * Use the values (<code>Double</code>) to compare objects (i.e., sorts by value).
     * @param reverse If <code>true</code>, reverse the natural sort order.
     */
    public void compareValues(boolean reverse) {
        this.clearFlag(COMPARE_KEY_VALUE);
        this.setFlag(COMPARE_VALUE);
        if (reverse)  {
            this.setFlag(REVERSE_VALUE_SORT);
        } else {
            this.clearFlag(REVERSE_VALUE_SORT);
        }
    }

    /**
     * Use the keys (<code>String</code>) and then values (<code>Double</code>) to compare objects.
     * @param reverseKey    If <code>true</code>, reverse the natural sort order of the key.
     * @param reverseValue  If <code>true</code>, reverse the natural sort ordeer of the value.
     */
    public void compareKeyValues(boolean reverseKey, boolean reverseValue) {
        this.clearFlag(COMPARE_VALUE);
        this.setFlag(COMPARE_KEY_VALUE);
        if (reverseKey)  {
            this.setFlag(REVERSE_KEY_SORT);
        } else {
            this.clearFlag(REVERSE_KEY_SORT);
        }
        if (reverseValue)  {
            this.setFlag(REVERSE_VALUE_SORT);
        } else {
            this.clearFlag(REVERSE_VALUE_SORT);
        }
    }

    /**
     * Reverse the natural sort order.
     */
    public void sort(boolean reverse) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void clear() {
        this.key = null;
        this.value = null;
        this.flags = 0x00;
    }

    /**
     * Returns key-tab-value
     */
    @Override
    public String toString() {
        return this.key + "\t" + Double.toString(value);
    }

    /**
     * Test if flag bit is set
     */
    private boolean flagSet(byte flag) {
        return ((this.flags & flag) != 0 ? true : false);
    }

    private void setFlag(byte flag) {
        this.flags |= flag;
    }

    private void clearFlag(byte flag) {
        this.flags &= (flag ^ Byte.MAX_VALUE);      // AND (flag XOR 0xFF)
    }

    // Methods required for Hadoop Partitioners (like the default HashPartioner)
    /** Returns true iff <code>o</code> is a KmerEntropyPairWritable with the same value. */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof StringDoublePairWritable)) {
            return false;
        }
        return (this.compareTo(o) == 0);
    }

    /**
     * Returns the hashCode of the key (<code>String</code>) or
     * the hashCode of the value (<code>Double</code>) if comparing values.
     */
    @Override
    public int hashCode() {
        // TODO: change this depending on the COMPARE FLAG
        if (this.flagSet(COMPARE_VALUE)) {
            return (this.hasValue()) ? this.value.hashCode():0;
        }
        // return key hash code if COMPARE_KEY_VALUE
        return this.key.hashCode();
    }

    // WritableComparable methods
    /**
     * Compares on the <code>String</code> (k-mer) value, but not weather it is local or not or its value.
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
     */
    @Override
    public int compareTo(Object o) {
        int revKey = (this.flagSet(REVERSE_KEY_SORT)) ? -1:1;
        int revValue = (this.flagSet(REVERSE_VALUE_SORT)) ? -1:1;
        StringDoublePairWritable other = (StringDoublePairWritable) o;
        if (this.flagSet(COMPARE_VALUE)) {
            // This will die if either is null and it should
            // http://java.sun.com/j2se/1.4.2/docs/api/java/lang/Comparable.html
            return revValue * this.value.compareTo(other.value);
        } else if (this.flagSet(COMPARE_KEY_VALUE)) {
            int cmp = this.key.compareTo(other.key);
            if (cmp != 0) {
                return revKey * cmp;
            }
            // equal keys, now compare by value
            return revValue * this.value.compareTo(other.value);
        } else {
            return revKey * this.key.compareTo(other.key);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(0);
        out.writeUTF(this.key);
        if (this.value != null) {               // checks real value
            this.setFlag(HAS_VALUE);
            out.writeByte(this.flags);
            out.writeDouble(this.value);
        } else {
            this.clearFlag(HAS_VALUE);
            out.writeByte(this.flags);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.clear();
        in.readByte();
        this.key = in.readUTF();
        this.flags = in.readByte();
        if (this.flagSet(HAS_VALUE)) {
            this.value = in.readDouble();
        } else {
            this.value = null;
        }
    }
}

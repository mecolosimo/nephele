/**
 * Created on April 2, 2009.
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

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

/**
 * A {@link WritableComparable} similar to {@link DoubleWritable}
 * but sorts in reverse order: highest to lowest.
 *
 * @author Marc Colosimo
 */
public class ReverseDoubleWritable implements WritableComparable {

    private double value = 0.0;

    /**
     * Register our comparator
     */
    static {
        WritableComparator.define(ReverseDoubleWritable.class, new Comparator());
    }

    public ReverseDoubleWritable() {
    }

    public ReverseDoubleWritable(double value) {
        set(value);
    }

    public void set(double value) {
        this.value = value;
    }

    public double get() {
        return this.value;
    }

    /**
     * Uses the value for the hasCode.
     * 
     * @return a <code>hash code</code> value for this object.
     */
    @Override
    public int hashCode() {
        /** Same as Double */
        long v = Double.doubleToLongBits(this.value);
        return (int)(v^(v>>>32));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ReverseDoubleWritable other = (ReverseDoubleWritable) obj;
        return (this.value == other.value) ? true : false;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value = in.readDouble();
    }

    /**
     * Returns the reverse order: largest to smallest
     */
    @Override
    public int compareTo(Object o) {
        ReverseDoubleWritable other = (ReverseDoubleWritable) o;
        return (value < other.value ? -1 : (value == other.value ? 0 : 1));
    }

    /** A WritableComparator optimized for ReverseDoubleWritable. */
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(ReverseDoubleWritable.class);
        }

        /**
         * Returns the reverse order sorting: largest to smallest.
         *
         * Optimization hook.  Override this to make SequenceFile.Sorter's scream.
         *
         * <p>The default implementation reads the data into two {@link
         * WritableComparable}s (using {@link Writable#readFields(DataInput)},
         * then calls {@link #compare(WritableComparable,WritableComparable)}.
         *
         * @param b1    first object bytes
         * @param s1    start position for WritableComparable's readField
         * @param l1    end position for object
         * @param b2    second object bytes (this could be the same as b1?)
         * @param s2    start position for WritableComparable's readField
         * @param l2    end position for object
         * @return
         */
        @Override
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            double thisValue = readDouble(b1, s1);
            double thatValue = readDouble(b2, s2);
            return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
        }
    }
}

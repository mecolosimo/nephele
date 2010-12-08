/**
 * Created on Feb 5, 2009.
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

package org.mitre.la.mapred.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.WritableComparable;

import org.mitre.la.SparseVector;

/**
 * A {@link WritableComparable} for storing {@link SparseVector}s sorting on their labels.
 *
 * @author Marc Colosimo
 */
public class SparseVectorWritable implements WritableComparable {

    private SparseVector sv;

    public SparseVectorWritable() {
        this.clear();
    }

    public SparseVectorWritable(SparseVector vector) {
        this.clear();
        this.sv = vector;
    }

    /**
     * Returns the SparseVector. This will return a new instance after each {@link #readFields}.
     */
    public SparseVector get() {
        return this.sv;
    }

    public void set(SparseVector vector) {
        this.clear();
        this.sv = vector;
    }

    public void clear() {
        this.sv = null;
    }

    // Hadoop WritableComparable methods
    @Override
    public int hashCode(){
        final String label = this.sv.getLabel();
        return (label == null) ? 0 : this.sv.getLabel().hashCode();
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param obj
     * @return  <code>true</code> if this object is the same as the obj
     *          argument; <code>false</code> otherwise.
     * @throws NullPointerException if this is missing a label (<code>null</code>).
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SparseVectorWritable other = (SparseVectorWritable) obj;
        if (this.sv == other.sv) {
            return true;
        }
        return this.sv.getLabel().equals(other.sv.getLabel());
    }


    /**
     * Compare them on their labels.
     *
     * @throws NullPointerException if either are missing a label (<code>null</code>).
     */
    @Override
    public int compareTo(Object obj) {
        return this.sv.getLabel().compareTo( ((SparseVectorWritable) obj).sv.getLabel());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(0);
        out.writeUTF(this.sv.getLabel());
        out.writeInt(this.sv.getCardinality());
        Map<Integer, Double> values = this.sv.getSparseMap();
        int length = values.size();
        out.writeInt(length);
        for(Map.Entry<Integer, Double> entry : values.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeDouble(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readByte();
        this.clear();
        String label = in.readUTF();
        Integer cardinality = in.readInt();
        this.sv = new SparseVector(label, cardinality);
        int length = in.readInt();
        for (int cv = 0; cv < length; cv++) {
            this.sv.add(in.readInt(),in.readDouble());
        }
    }

}

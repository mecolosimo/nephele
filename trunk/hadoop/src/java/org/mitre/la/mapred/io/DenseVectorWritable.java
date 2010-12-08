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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;

import org.mitre.la.DenseVector;

/**
 * A @link{WritableComparable} class for storing @link{DenseVectors}.
 *
 * @author Marc Colosimo
 */
public class DenseVectorWritable implements WritableComparable {

    private static final Log LOG = LogFactory.getLog(DenseVectorWritable.class);

    private DenseVector dv;

    /**
     * Constructs an empty <code>Writable</code>.
     *
     * <p>Use this for reading vectors back in.</p>
     */
    public DenseVectorWritable() {
        this(null);
    }


    public DenseVectorWritable(DenseVector V) {
        this.dv = V;
    }

    public void clear() {
        this.dv = null;
    }

    /**
     * Return the BasicVector.
     *
     * This is not part of the Writable inferface, but everyone uses it!
     */
    public DenseVector get() {
        return this.dv;
    }

    public void set(DenseVector V) {
        this.dv = V;
    }
    
    // Hadoop WritableComparable methods
    @Override
    public int hashCode(){
        final String label = this.dv.getLabel();
        return (label == null) ? 0 : this.dv.getLabel().hashCode();
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
        final DenseVectorWritable other = (DenseVectorWritable) obj;
        if (this.dv == other.dv) {
            return true;
        }
        return this.dv.getLabel().equals(other.dv.getLabel());
    }


    /**
     * Compare them on their labels.
     *
     * @throws NullPointerException if either are missing a label (<code>null</code>).
     */
    @Override
    public int compareTo(Object o) {
        return this.dv.getLabel().compareTo( ((DenseVectorWritable) o).dv.getLabel());
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        int length = this.dv.getCardinality();
        out.writeByte(0);                   // Space for versioning
        out.writeUTF(this.dv.getLabel());
        out.writeInt(length);
        for (int i = 0; i < length; i++) {
            out.writeDouble(this.dv.get(i));
        }
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        in.readByte();
        String label = in.readUTF();
        int length = in.readInt();
        double[] v = new double[length];
        for (int i = 0; i < length; i++) {
            v[i] = in.readDouble();
        }
        this.dv = new DenseVector(label, v);
    }

}

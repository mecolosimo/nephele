/**
 * Created on April 6, 2009.
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
 * A {@link WritableComparable} class for storing Integer-Double tuples.
 * <P>This compares on the value of the key (<code>Integer</code>) value.
 * 
 * @author Marc Colosimo
 */
public class IntegerDoublePairWritable implements WritableComparable {

    private Integer key;
    private Double value;

    public IntegerDoublePairWritable() {
        this.clear();
    }

    public IntegerDoublePairWritable(Integer k, Double v) {
        this.clear();
        this.set(k, v);
    }

    public Integer getKey() {
        return this.key;
    }

    public Double getValue() {
        return this.value;
    }

    public void set(Integer k, Double v) {
        this.key = k;
        this.value = v;
    }

    public void clear() {
        this.key = null;
        this.value = null;
    }

    @Override
    public int hashCode() {
        return this.key;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final IntegerDoublePairWritable other = (IntegerDoublePairWritable) obj;
        if (this.key != other.key && (this.key == null || !this.key.equals(other.key))) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(Object o) {
        return this.key.compareTo(((IntegerDoublePairWritable) o).key);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.key);
        out.writeDouble(this.value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.key = in.readInt();
        this.value = in.readDouble();
    }
}

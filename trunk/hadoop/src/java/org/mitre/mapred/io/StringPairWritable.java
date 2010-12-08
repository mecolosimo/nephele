/*
 * Created on October 12, 2009.
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
 * A @link{WritableComparable} class for storing <code>String</code>/<code>String</code> pairs, 
 * that is sorted on the first String, then on the second.
 * 
 * @author Marc Colosimo
 */
public class StringPairWritable implements WritableComparable {
    private String first;   // row
    private String second;   // column

    public StringPairWritable() {
        this.first = null;
        this.second = null;
    }

    public StringPairWritable(String firstLabel, String secondLabel ) {
        this.first = firstLabel;
        this.second = secondLabel;
    }

    public String getFirstLabel() {
        return this.first;
    }

    public void setFirstLabel(String label) {
        this.first = label;
    }

    public String getSecondLabel() {
        return this.second;
    }

    public void setSecondLabel(String label) {
        this.second = label;
    }

    /**
     * Returns the hashCode of the first label (<code>String</code>).
     */
    @Override
    public int hashCode() {
        return this.first != null ? this.first.hashCode():0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        /*
        final StringPairWritable other = (StringPairWritable) obj;
        if ((this.first == null) ? (other.first != null) : !this.first.equals(other.first)) {
            return false;
        }

        if ((this.second == null) ? (other.second != null) : !this.second.equals(other.second)) {
            return false;
        }
        return true;
         * */
        return (this.compareTo(obj) == 0);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(0);
        out.writeUTF(this.first);
        out.writeUTF(this.second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readByte();
        this.first = in.readUTF();
        this.second = in.readUTF();
    }

    // WritableComparable methods
    /**
     * Compares on the <code>firstLabel</code> value and if equal compares on the <code>secondLabel</code>
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
     */
    @Override
    public int compareTo(Object obj) {
        StringPairWritable other = (StringPairWritable) obj;
        int ret = this.first.compareTo(other.first);
        return ret != 0 ? ret: this.second.compareTo(other.second);
    }

}

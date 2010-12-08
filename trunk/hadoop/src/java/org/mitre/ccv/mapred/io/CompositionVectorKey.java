/**
 * Created on March 31, 2009.
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
import org.apache.hadoop.io.WritableComparable;

/**
 * A @link{WritableComparable} class that sorts by name then window size (k-mer length).
 *
 * @author Marc Colosimo
 */
public class CompositionVectorKey implements WritableComparable  {

    private String name;
    private Integer windowSize;
    
    public CompositionVectorKey() {
        this.set(null,null);
    }
    
    public CompositionVectorKey(String name, Integer windowSize) {
        this.set(name, windowSize);
    }
    
    public void set(String name, Integer windowSize) {
        this.name = name;
        this.windowSize = windowSize;
    }
    
    public String getName() {
        return this.name;
    }
    
    public Integer getWindowSize() {
        return this.windowSize;
    }
    
    // Methods required for Hadoop Partitioners (like the default HashPartioner)
    /** Returns true iff <code>o</code> is a KmerEntropyPairWritable with the same value. */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CompositionVectorKey)) {
            return false;
        }
        return (this.compareTo(o) == 0);
    }

    /**
     * Returns the hashCode of the name (<code>String</code>), which is used by {@link HashPartioner}
     */
    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    // WritableComparable Methods
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.name);
        out.writeInt(this.windowSize);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       this.name = in.readUTF();
       this.windowSize = in.readInt();
    }

    /**
     * Compares the name first and if they are the same then by their window size.
     *
     * @return
     */
    @Override
    public int compareTo(Object o) {
        CompositionVectorKey thatValue =  (CompositionVectorKey) o;
        CompositionVectorKey thisValue = this;
        int cmp = thisValue.name.compareTo(thatValue.name);
        if (cmp != 0) {
            return cmp;
        }
        cmp = thisValue.windowSize.compareTo(thatValue.windowSize);
        return cmp;
    }
}

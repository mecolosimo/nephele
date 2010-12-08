/**
 * Created on April 3, 2009.
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
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;

import org.mitre.mapred.io.StringDoublePairWritable;

/**
 * A {@link Writable} class for storing features (k-mer/value pairs as {@link StringDoublePairWritable}) in ordered added.
 *
 * @author Marc Colosimo
 */
public class FeatureSetWritable implements Writable {

    private TreeSet<StringDoublePairWritable> vector = new TreeSet<StringDoublePairWritable>();
    private String name;

    public FeatureSetWritable() {
        this.clear();
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void add(StringDoublePairWritable w) {
        this.add(w);
    }

    /**
     * Returns the underlining @link{ArrayList}
     */
    public TreeSet<StringDoublePairWritable> getVector() {
        return this.vector;
    }


    public void clear() {
        this.name = null;
        this.vector.clear();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.name);
        out.writeInt(this.vector.size());
        for (StringDoublePairWritable w : this.vector ) {
            w.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        int length = in.readInt();
        this.vector.clear();
        for (int cv = 0; cv < length; cv++) {
            StringDoublePairWritable w = new StringDoublePairWritable();
            w.readFields(in);
            this.vector.add(w);
        }
    }
}

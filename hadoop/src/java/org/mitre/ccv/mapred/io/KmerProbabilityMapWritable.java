/**
 * Created on March 25, 2009.
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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.Writable;

/**
 * A Hadoop serializable object for keeping track of k-mers and their frquencies, basically a tuple store.
 *
 * <p>This can be used for any <code>value</code> type in the Hadoop Map-Reduce
 * framework.</p>
 *
 * NOTE: As of Hadoop-19, what the documentation says how to use a Writable and
 * how the ReduceTask populates its Iterator is not correct.
 * @author Marc Colosimo
 */
public class KmerProbabilityMapWritable implements Writable {

    private TreeMap<String, Double> map;
    private String key;

    public KmerProbabilityMapWritable() {
        this.map = new TreeMap<String, Double>();
        this.key = null;
    }

    /**
     * New object with the given base key
     */
    public KmerProbabilityMapWritable(String kmer, Double frequency) {
        this.map = new TreeMap<String, Double>();
        this.key = kmer;
        this.map.put(kmer, frequency);
    }

    public void setKey(String kmer) {
        this.key = kmer;
    }
    
    /**
     * Set the kmer and its frequency.
     * 
     * @return previous value associated with specified key, or <code>null</code> if there
     * was no mapping for key. A <code>null</code> return can also indicate that the map
     * previously associated <code>null</code> with the specified key.
     */
    public Double set(String kmer, Double frequency) {
        return this.map.put(kmer, frequency);
    }

    /**
     * Returns <code>Map</code>.
     */
    public Map get() {
        return this.map;
    }

    /**
     * The key might not actually be in this map.
     */
    public String getKey() {
        return this.key;
    }

    public void clear() {
        this.map.clear();
        this.key = null;
    }

    /**
     * Returns the value (frequency) for the k-mer or <code>null</code> if no value is stored.
     */
    public Double getValue(String kmer) {
        return this.map.get(kmer);
    }

    public Boolean containsKmer(String kmer) {
        return this.map.containsKey(kmer);
    }

    // Hadoop Writable methods
    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(0);                           // Space for versioning
        out.writeUTF(this.key);
        int length = this.map.size();
        out.writeInt(length);
        for (Iterator<Entry<String,Double>> iter = this.map.entrySet().iterator(); iter.hasNext(); ) {
            Entry<String, Double> entry = iter.next();
            out.writeUTF(entry.getKey());
            out.writeDouble(entry.getValue());
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
        this.key = in.readUTF();
        int length = in.readInt();
        this.map = new TreeMap<String, Double>();
        for (int cv = 0; cv < length; cv++) {
            this.map.put(in.readUTF(), in.readDouble());
        }
    }
}

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

/**
 *
 * @author Marc Colosimo
 */
public class CompositionVectorWritable implements Writable {

    private HashMap<String, Double> map = new HashMap<String, Double>();
    private String name;
    private Integer windowSize;

    public CompositionVectorWritable() {
        this.clear();
    }

    public CompositionVectorWritable(String name, Integer windowSize) {
        this.clear();
        this.name = name;
        this.windowSize = windowSize;
    }

    public CompositionVectorWritable(String name, Integer windowSize, Map<String, Double> compositionVector){
        this.clear();
        this.name = name;
        this.windowSize = windowSize;
        this.map.putAll(compositionVector);
    }
    
    public void clear() {
        this.map.clear();
        this.name = null;
        this.windowSize = null;
    }

    /**
     * Copies all of the mappings from the specified <code>map</code> to this map.
     * These mappings will replace any mappings that this map had for any of 
     * the keys currently in the specified map.
     */
    public void putAll(Map<String, Double> compositionVector) {
        this.map.putAll(compositionVector);
    }
    
    /**
     * Add a new kmer-value pair.
     *
     * @return previous <code>value</code> associated with specified </code>key</code>,
     *      or <code>null</code> if there was no mapping for </code>key</code>.
     *      A <code>null</code> return can also indicate that the map previously
     *      associated <code>null</code> with the specified key.
     */
    public Double add(String k, Double v) {
        return this.map.put(k,v);
    }

    /**
     * Returns the pi values for the composition vector.
     * 
     * This is might be a copy of the data or not. So <B>DO NOT</B> modify it.
     */
    public Map<String, Double> getCompositionVector() {
        return this.map;
    }

    public Boolean containsKey(String k) {
        return this.map.containsKey(k);
    }

    public Boolean containsValue(Double v) {
        return this.map.containsValue(v);
    }

    public Double getValue(String k) {
        return this.map.get(k);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(0);                       // place holder
        out.writeUTF(this.name);
        out.writeInt(this.windowSize);
        int length = this.map.size();
        out.writeInt(length);
        //System.err.printf("Writing %s, %d with %d k-mers\n", this.name, this.windowSize, length);
        for (Iterator<Entry<String, Double>> iter = this.map.entrySet().iterator(); iter.hasNext(); ) {

            Entry<String, Double> entry = iter.next();
            out.writeUTF(entry.getKey());
            out.writeDouble(entry.getValue());
            //System.err.printf("\t%s\t%f", entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readByte();
        this.name = in.readUTF();
        this.windowSize = in.readInt();
        this.map.clear();
        int length = in.readInt();
        for (int cv = 0; cv < length; cv++) {
            this.map.put(in.readUTF(), in.readDouble());
        }
    }

}

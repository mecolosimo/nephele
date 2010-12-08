/**
 * Created on March 24, 2009.
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Writable;

/**
 * A Hadoop serializable object for keeping track of k-mer counts and parent k(+)-mers,
 * This implements a simple, efficient, serialization protocol, based on {@link DataInput} and
 * {@link DataOutput}.
 *
 * <p>For example, the count for "T" is saved along with "AT", "ATG" as parents</p>
 *
 * <p>This can be used for any <code>value</code> type in the Hadoop Map-Reduce
 * framework.</p>
 *
 * NOTE: As of Hadoop-19, what the documentation says how to use a Writable and
 * how the ReduceTask populates its Iterator is  not correct.
 * 
 * @author Marc Colosimo
 */
public class KmerCountWritable implements Writable {

    private String kmer;
    private int cnt;
    private HashSet parentNmers;

    /**
     * Constructor of empty object
     */
    public KmerCountWritable() {
        this(null, 0);
    }

    public KmerCountWritable(String kmer) {
        this(kmer, 0);
    }

    /**
     * Constructor
     *
     * @param kmer  the kmer
     * @param count the count of the kmer
     */
    public KmerCountWritable(String kmer, int count) {
        this.kmer = kmer;
        this.cnt = count;
        this.parentNmers = new HashSet<String>();
    }
 
    /**
     * Returns the kmer
     */
    public String get() {
        return this.kmer;
    }

    /**
     * Returns the count of this kmer
     */
    public Integer getCount() {
        return this.cnt;
    }

    /**
     * Returns the a set of parent kmers
     */
    public Set<String> getParents() {
        return this.parentNmers;
    }

    /**
     * Set the kmer value and clearing the frequency and parents
     */
    public void set(String kmer) {
        this.set(kmer, 0);
    }

    /**
     * Set the kmer and frequency values and clearing the parents
     */
    public void set(String kmer, Integer count) {
        this.kmer = kmer;
        this.cnt = count;
        this.parentNmers.clear();
    }

    /**
     * Set the frequency
     */
    public void setFrequency(int count) {
        this.cnt = count;
    }

    /**
     * Adds a parent kmer.
     *
     * @returns <code>true</code> if the set did not already contain the specified element.
     */
    public boolean addParent(String kmer) {
        return this.parentNmers.add(kmer);
    }

    /**
     * @return <code>true</code> if this collection changed as a result of the call.
     */
    public boolean addParent(Set kmers) {
        return this.parentNmers.addAll(kmers);
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
        out.writeByte(0);                   // Space for versioning
        out.writeUTF(this.kmer);
        out.writeInt(this.cnt);
        int length = this.parentNmers.size();
        out.writeInt(length);
        for (Iterator<String> iter = this.parentNmers.iterator(); iter.hasNext();) {
            out.writeUTF(iter.next());
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
        this.kmer = in.readUTF();
        this.cnt = in.readInt();
        this.parentNmers = new HashSet<String>();
        int length = in.readInt();
        for (int cv = 0; cv < length; cv++) {
            this.parentNmers.add(in.readUTF());
        }
    }
}

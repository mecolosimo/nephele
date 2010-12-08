/**
 * Created on April 7, 2009.
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
package org.mitre.la;

import java.util.HashMap;
import java.util.Map;

/**
 * A sparse {@link Vector} implementation (only non-zero values are stored).
 * 
 * @author Marc Colosimo
 */
public class SparseVector extends AbstractVector  {

    private String label;
    private HashMap<Integer, Double> values = new HashMap<Integer, Double>();
    private Integer cardinality;

    public SparseVector() {
    }

    public SparseVector(String label, int cardinality) {
        this.clear();
        this.label = label;
        this.cardinality = cardinality;
    }

    /**
     * Constructs a new zero filled <code>Vector</code> of cardinality.
     *
     * @param length
     */
    public SparseVector(int cardinality) {
        this.clear();
        this.cardinality = cardinality;
    }

    /**
     * Constructor for making a sparse vector from the given array.
     * <P>Zero value are not stored.</P>
     *
     * @param label
     * @param vector
     */
    public SparseVector(String label, double[] vector) {
        this.clear();
        this.label = label;
        this.cardinality = vector.length;
        for (int cv = 0; cv < this.cardinality; cv++) {
            if (vector[cv] != 0.0) {
                this.values.put(cv, vector[cv]);
            }
        }
    }

    public SparseVector(double[] vector) {
        this(null, vector);
    }

    private void clear() {
        this.values.clear();
        this.label = null;
        this.cardinality = 0;
    }

    /**
     * Returns the underlining map of non-zero values.
     */
    public Map<Integer, Double> getSparseMap() {
        return this.values;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public int getCardinality() {
        return this.cardinality;
    }

    /**
     * Gets the value of index
     *
     * @param index
     * @return v(index)
     * @throws ArrayIndexOutOfBoundsException
     */
    @Override
    public double get(int index) throws ArrayIndexOutOfBoundsException {
        checkBounds(index);
        Double value = this.values.get(index);
        return value == null ? 0.0 : value;
    }

    /**
     * Sets the value of index
     *
     * @param index
     * @param value
     * @throws ArrayIndexOutOfBoundsException
     */
    @Override
    public void set(int index, double value) throws ArrayIndexOutOfBoundsException {
        checkBounds(index);
        if (value != 0.0) {
            this.values.put(index, value);
        }
    }

    /**
     * Adds the value to the one at the index
     *
     * @param index
     * @param value
     * @throws ArrayIndexOutOfBoundsException
     */
    @Override
    public void add(int index, double value) throws ArrayIndexOutOfBoundsException {
        checkBounds(index);
        Double v = this.get(index); 
        this.set(index, v + value);
    }

    /**
     * Z = V + X
     *
     * @param x another vector
     * @return Z = V + X
     * @throws VectorLengthException
     */
    @Override
    public Vector plus(Vector X) throws VectorLengthException {
        this.checkCardinality(X);
        int length = this.getCardinality();
        SparseVector sv = new SparseVector(length);
        for (int i = 0; i < length; i++) {
            // should be have this as sv.set() then sv.add?
            sv.set(i, this.get(i) + X.get(i));
        }
        return sv;
    }

    /**
     * V = V + X
     *
     * @param X another vector
     * @throws VectorLengthException
     */
    @Override
    public void plusEquals(Vector X) throws VectorLengthException {
        this.checkCardinality(X);
        int length = this.getCardinality();
        for (int i = 0; i < length; i++) {
            //this.V[i] += X.get(i);
            this.add(i, X.get(i));
        }
    }

    /**
     * V = V + X
     *
     * @param X another vector
     * @throws VectorLengthException
     */
    public void plusEquals(SparseVector X) throws VectorLengthException {
        this.checkCardinality(X);
        // Only need to add the values from X into this
        for (Integer i : X.values.keySet()) {
            this.add(i, X.values.get(i));
        }
    }

    @Override
    public Vector minus(Vector X) throws VectorLengthException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void minusEquals(Vector X) throws VectorLengthException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * V = alpha*V
     *
     * @param alpha
     * @return V = alpha*V
     */
    @Override
    public Vector scale(double alpha) {
        int length = this.getCardinality();
        SparseVector sv = new SparseVector(length);
        for (int i = 0; i < length; i++ ) {
            double value = this.get(i);
            if (value != 0.0) {
                sv.set(i, value * alpha);
            }
        }
        return sv;
    }

    @Override
    public Vector subVector(int i0, int i1) throws ArrayIndexOutOfBoundsException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private void checkBounds(int index) throws ArrayIndexOutOfBoundsException {
        if (index >= this.cardinality || index < 0) {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    private void checkCardinality(Vector X) throws VectorLengthException {
        if (this.getCardinality() != X.getCardinality()) {
            throw new VectorLengthException("Vector lengths are unequal!");
        }
    }
}

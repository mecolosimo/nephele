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

package org.mitre.la;

/**
 * A {@link Vector} class for dense vectors (all values are stored).
 *
 * @author Marc Colosimo
 */
public class DenseVector extends AbstractVector {

        /** our vector */
    private double[] V;
    private String label;

    /**
     * Constructor required for Writable
     */
    public DenseVector() {
        this.clear();
    }

    /**
     * Constructs a new zero filled Vector with the given label and cardinality.
     */
    public DenseVector(String label, int cardinality) {
        this.clear();
        this.label = label;
        this.V = new double[cardinality];
    }

    /**
     * Constructs a new zero filled <code>Vector</code> of cardinality.
     *
     * @param length
     */
    public DenseVector(int cardinality) {
        this.clear();
        this.V = new double[cardinality];
    }

    public DenseVector(String label, double[] vector) {
        this.clear();
        this.label = label;
        this.V = vector;
    }

    public DenseVector(double[] vector) {
        this.clear();
        this.V = vector;
    }
    
    public DenseVector(Vector vector) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    //public double [] getVectorCopy() {return this.V;}

    private void clear() {
        this.label = null;
        this.V = null;
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
        return this.V.length;
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
        return this.V[index];
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
        this.V[index] = value;
    }

    /**
     * Adds the value to the one at the index
     *
     * @param index
     * @param value
     * @throws ArrayIndexOutOfBoundsException
     */
    @Override
    public void add(int index, double value) {
        this.V[index] += value;
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
        int length = this.V.length;
        double[] Z = new double[length];
        for (int i=0; i < length; i++) {
            Z[i] = this.V[i] + X.get(i);
        }
        return new DenseVector(Z);
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
        int length = this.V.length;
        for (int i=0; i < length; i++) {
            this.V[i] += X.get(i);
        }
    }

    /**
     * Z = V - X
     *
     * @param x another vector
     * @return Z = V - X
     * @throws VectorLengthException
     */
    @Override
    public Vector minus(Vector X) throws VectorLengthException {
        this.checkCardinality(X);
        int length = this.V.length;
        double[] Z = new double[length];
        for (int i=0; i < length; i++) {
            Z[i] = this.V[i] - X.get(i);
        }
        return new DenseVector(Z);
    }

    /**
     * V = V - X
     *
     * @param x another vector
     * @throws VectorLengthException
     */
    @Override
    public void minusEquals(Vector X) throws VectorLengthException {
        this.checkCardinality(X);
        int length = this.V.length;
        for (int i=0; i < length; i++) {
            this.V[i] -= X.get(i);
        }
    }

    @Override
    public Vector scale(double alpha) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Vector subVector(int i0, int i1) throws ArrayIndexOutOfBoundsException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public double calculateInfinityNorm() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private void checkCardinality(Vector X) throws VectorLengthException {
        if ( this.V.length != X.getCardinality() ) {
            throw new VectorLengthException("Vector lengths are unequal!");
        }
    }
}

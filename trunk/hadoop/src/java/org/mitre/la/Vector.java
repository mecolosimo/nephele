/**
 * Created on Feb 3, 2009
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
 * An interface for supporting vectors based on JAMA and other Linear Algrabra code.
 *
 * @author Marc Colosimo
 */
public interface Vector {

    /**
     * Gets the label (name) for the vector.
     */
    public String getLabel();
    
    /**
     * Set the label (name) for the vector.
     */
    public void setLabel(String label);
    
    /**
     * Gets the cardinality of the vector
     */
    public int getCardinality();


    /**
     * Gets the length (magnitude) of the vector.
     */
    public double length();
    
    /**
     * Gets the value of index
     *
     * @param index
     * @return v(index)
     * @throws ArrayIndexOutOfBoundsException
     */
    public double get(int index) throws ArrayIndexOutOfBoundsException;

    /**
     * Sets the value of index
     *
     * @param index
     * @param value
     * @throws ArrayIndexOutOfBoundsException
     */
    public void set(int index, double value) throws ArrayIndexOutOfBoundsException;

    /**
     * Adds the value to the one at the index
     *
     * @param index
     * @param value
     * @throws ArrayIndexOutOfBoundsException
     */
    public void add(int index, double value) throws ArrayIndexOutOfBoundsException;

    /**
     * Z = V + X
     *
     * @param x another vector
     * @return Z = V + X
     * @throws VectorLengthException
     */
    public Vector plus(Vector X) throws VectorLengthException;

    /**
     * V = V + X
     *
     * @param X another vector
     * @throws VectorLengthException
     */
    public void plusEquals(Vector X) throws VectorLengthException;

    /**
     * Z = V - X
     *
     * @param x another vector
     * @return Z = V - X
     * @throws VectorLengthException
     */
    public Vector minus(Vector X) throws VectorLengthException;

    /**
     * V = V - X
     *
     * @param x another vector
     * @throws VectorLengthException
     */
    public void minusEquals(Vector X) throws VectorLengthException;

    /**
     * Dot(scalar) product: V dot X
     *
     * @param X another vector
     * @return scalar value of V dot X
     * @throws VectorLengthException
     */
    public double dot(Vector X) throws VectorLengthException; // should be sum of Ai * Vi

    /**
     * V = alpha*V
     *
     * @param alpha
     * @return V = alpha*V
     */
    public Vector scale(double alpha);

    /**
     * Returns a sub-vector.
     *
     * @param i0 the index of the first element
     * @param i1 the index of the last element
     * @return v[i0:i1]
     * @throws ArrayIndexOutOfBoundsException
     */
    public Vector subVector(int i0, int i1) throws ArrayIndexOutOfBoundsException;

    /**
     * Returns the sum of the absolute values of the entries (norm1).
     */
    public double calculateNorm1(); 

    /**
     * Returns the square root of the sum of the squares (Euclidean vector norm  or norm2).
     */
    public double calculateNorm2();
    
    /**
     * Returns the largest entry in absolute value
     */
    public double calculateInfinityNorm();

    /**
     * Returns the angle theta between this vector and X
     * 
     * @param X
     * @return
     * @throws VectorLengthException
     */
    public double cosine(Vector X) throws VectorLengthException;

    /**
     * Returns a string representation of the vector
     *
     * @param width      Column width.
     * @param digits     Number of digits after the decimal.
     */
    public String vector2String(final int width, final int digits) ;
}


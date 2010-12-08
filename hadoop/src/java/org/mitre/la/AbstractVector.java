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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;


/**
 * An abstact @link{Vector} class.
 *
 * @author Marc Colosimo
 */
public abstract class AbstractVector implements Vector {

    /**
     * Gets the length (magnitude) of the vector.
     */
    @Override
    public double length() {
        double l = 0.0;
        try {
            l = Math.sqrt(this.dot(this));
        } catch (VectorLengthException ex) {
            // Shouldn't be here!
        }
        return l;
    }

    /**
     * Dot(scalar) product: V dot X
     *
     * @param X another vector
     * @return scalar value of V dot X
     * @throws VectorLengthException
     */
    @Override
    public double dot(Vector X) throws VectorLengthException {
        this.checkCardinality(X);
        double s = 0.0;
        for (int i = 0; i < this.getCardinality(); i++) {
            s += this.get(i) * X.get(i);
        }
        return s;
    }

    /**
     * Returns the sum of the absolute values of the entries (norm1).
     */
    @Override
    public double calculateNorm1() {
        double sum = 0.0;
        for (int i = 0; i < this.getCardinality(); i++) {
            // Is this numerically stable?
            sum += Math.abs(this.get(i));
        }
        return sum;
    }

    @Override
    public double calculateNorm2() {
        double square_sum = 0.0;
        for (int i = 0; i < this.getCardinality(); i++) {
            double value = this.get(i);
            square_sum += value * value;   // sum (abs(value*value)...)
        }
        return Math.sqrt(square_sum);
    }

    /**
     * Returns the largest entry in absolute value
     */
    @Override
    public double calculateInfinityNorm() {
        double abs_max = 0.0;
        for (int i = 0; i < this.getCardinality(); i++) {
            abs_max = Math.max(abs_max, Math.abs(this.get(i)));
        }
        return abs_max;
    }

    @Override
    public double cosine(Vector X) throws VectorLengthException {
        return this.dot(X) / (this.calculateNorm2() * X.calculateNorm2());
    }

    /**
     * Returns a string representation of the vector
     *
     * @param width      Column width (padding, set this to 0 for no extra padding).
     * @param digits     Number of digits after the decimal.
     */
    @Override
    public String vector2String(final int width, final int digits) {
        DecimalFormat format = new DecimalFormat();
        format.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.US));
        format.setMinimumIntegerDigits(1);
        format.setMaximumFractionDigits(digits);
        format.setMinimumFractionDigits(digits);
        format.setGroupingUsed(false);

        StringBuilder output = new StringBuilder();
        for (int i = 0; i < this.getCardinality(); i++) {
            String s = format.format(this.get(i)); // format the number
            final int padding = Math.max(1, width - s.length()); // At _least_ 1 space
            for (int k = 0; k < padding; k++) {
                output.append(' ');
            }
            output.append(s);
        }
        return output.toString();
    }

    private void checkCardinality(Vector X) throws VectorLengthException {
        if (this.getCardinality() != X.getCardinality()) {
            throw new VectorLengthException("Vector lengths are unequal!");
        }
    }

    private void checkBounds(int index) throws ArrayIndexOutOfBoundsException {
        if (index >= this.getCardinality() || index < 0) {
            throw new ArrayIndexOutOfBoundsException();
        }
    }
}

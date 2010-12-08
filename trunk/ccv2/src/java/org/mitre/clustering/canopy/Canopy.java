/*
 * Created on 14 December 2009.
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

package org.mitre.clustering.canopy;

import java.util.LinkedHashSet;
import java.util.Iterator;

/**
 * This class hold the contents of a canopy.
 *
 * @author Marc Colosimo
 */
public class Canopy<T> implements Iterable{
    // want this to extend/implement an iterator
    private T founder;
    private LinkedHashSet<T> members;

    public Canopy(T point) {
        this.founder = point;
        this.members = new LinkedHashSet<T>();
        this.members.add(point);
    }

    /**
     * Adds the given point (douplicates are ignored)
     */
    public void add(T point) {
        this.members.add(point);
    }

    public T getFounder() {
        return this.founder;
    }
    
    /**
     * Returns an iterator over members (in the order they were added)
     */
    public Iterator<T> iterator() {
        return members.iterator();
    }
}

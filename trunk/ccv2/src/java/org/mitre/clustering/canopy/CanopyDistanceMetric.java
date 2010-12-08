/*
 * Created on 13 December 2009.
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

/**
 * Interface for the "cheap" or "exact" distance metric used to create canopy
 * clusters or place samples into clusters.
 *
 * @author Marc Colosimo
 */
public interface CanopyDistanceMetric<T> {

    double distance(T o1, T o2);
}

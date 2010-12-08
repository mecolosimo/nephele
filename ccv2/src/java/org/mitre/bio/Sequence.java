/**
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

package org.mitre.bio;

/**
 * Class describing a simple sequence.
 * 
 * @author Matt Peterson
 */
public class Sequence {
    
    private String name;
    private String description;
    private String sequence;
    
    public Sequence(String name, String description, String seqString) {
        this.name = name;
        this.description = description;
        this.sequence = seqString;
    }
    
    
    /**
     * Get the name of the sequence
     */
    public String getName() {
        return name;
    }
    
    /**
     * Get the description of the sequence
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Get the sequence data
     */
    public String seqString() {
        return sequence;
    }

    /**
     * Set the sequence name
     */
    public void setName(String n) {
        name = n;
    }

    /**
     * Set the description of the sequence
     */
    public void setDescription(String n) {
        description = n;
    }

}

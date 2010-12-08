/**
 * Updated on April 17, 2009.
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

package org.mitre.bio.io;

import org.mitre.bio.Sequence;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Simple FASTA file parser.
 *
 * @author Matt Peterson
 */
public class FastaIterator extends SequenceIterator {

    private static final Log LOG = LogFactory.getLog("FastaIterator");

    private Sequence nextSequence = null;
    private boolean returnSeq = false;
    private BufferedReader buffer;
    private List<String> dirList = null;
    
    
    public FastaIterator(File file) {
        if ( file.isDirectory() ) {
            this.buffer = null;
            this.dirList = new ArrayList<String>();
        } else {
            try {
                this.buffer = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException ex) {
                LOG.fatal("File '" + file.getPath() + "' was not found!");
                this.buffer = null;
            }
        }
    }
    
    public FastaIterator(FileReader fr) {
        buffer = new BufferedReader(fr);
    }
    
    public FastaIterator(BufferedReader br) {
       buffer = br;
    }
    

    public Sequence next() {
        if (!returnSeq) {
            if (!hasNext() ) {
                throw new NoSuchElementException();
            }
        }
        returnSeq = false;
        return nextSequence;
    }

    /**
     * This reads in the next sequence parsing the header.
     * <P>
     * The header is processed into two parts, (1) name and (2) description, based
     * on the presence of a space.
     * <blockquote>
     * >gb1000|testgene this is a test
     * </blockquote>
     * name will be ">gb1000|testgene"
     * description will be "this is a test"
     *
     * @return <code>true</code> if we successfully read in a sequence.
     */
    public boolean hasNext() {
        
        /** Check to see if we are reading fasta files from a directory */
        if (this.dirList != null) {
            return this.hasNextFile();
        }
        
        /** Do we even have a buffer? */
        if (this.buffer == null) {
            return false;
        }
        
        /** Are we in the middle of reading a sequence? */
        boolean reading = false;
        
        /** Get to the next sequence */
        while (reading == false) {
            try {
                int first = buffer.read();
                if (first == -1) {
                    return false;
                }

                if ((char) first == '>') {
                    reading = true;
                }
            } catch (IOException ioe) {
                LOG.warn(ioe);
                return false;
            }
        }
        
        /*
         * Get header information
         */
        String header = null;
        String name;
        String desc;
        
        try {
            header = buffer.readLine();
        } catch(IOException ioe) {
            LOG.warn(ioe);
            return false;
        }
        
        int endname = header.indexOf(" ");
        if (endname == -1) {
            name = header;
            desc = "";
        } else {
            name = header.substring(0, endname);
            desc = header.substring(endname+1, header.length());
        }
        
        StringBuilder sb = new StringBuilder();
        while (reading) {
            try {
                buffer.mark(2);
                int first = buffer.read();
                if (first == -1) {
                    nextSequence = new Sequence(name, desc, sb.toString());
                    returnSeq = true;
                    return true;
                }
                
                if ((char) first == '>') {
                    buffer.reset();
                    nextSequence = new Sequence(name, desc, sb.toString());
                    returnSeq = true;
                    return true;
                } else {
                    buffer.reset();
                    sb.append(buffer.readLine().trim());
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                return false;
            }
        }
        
        LOG.warn("There's been an error in logic!");
        return false;
    }
   
    
    private boolean hasNextFile() {
        return false;
    }

}

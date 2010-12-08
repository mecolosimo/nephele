/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mitre.bio.io;

import org.mitre.bio.Sequence;
import java.util.Iterator;


/**
 * Interface describing sequence iterator
 * @author mpeterson
 */
public abstract class SequenceIterator implements Iterator {
    
    /**
     * Returns the next sequence
     * @return
     */
    public abstract Sequence next();
    
    public abstract boolean hasNext();
    
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove Sequence");
    }
    

}

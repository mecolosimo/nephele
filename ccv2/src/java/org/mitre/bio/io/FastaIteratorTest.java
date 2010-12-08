/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mitre.bio.io;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;

import org.mitre.bio.Sequence;


/**
 *
 * @author mpeterson
 */
public class FastaIteratorTest {
    
    public static void main(String args[]) {
        
        String filename = args[0];
        
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            FastaIterator fi = new FastaIterator(br);
            
            while (fi.hasNext()) {
                
                Sequence s = fi.next();
                System.out.println(">" + s.getName());
                System.out.println(s.seqString());
            }
            
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        
    }

}

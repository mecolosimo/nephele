/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mitre.bio.utils;

/**
 *
 * @author MPETERSON
 */
public class MathTools {
    public static double getMedian(double[] array) {
        java.util.Arrays.sort(array);

    	int arrayLength = array.length;
        
    	if (arrayLength % 2 == 0) {
                int n = (arrayLength + 1)/2;
    		return (array[n] + array[n+1])/2.0;
    	} else {
                int n = arrayLength/2;
    		return array[n];
    	}
    }
}

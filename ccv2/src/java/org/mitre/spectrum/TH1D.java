/**
 * Created on 23 December 2009.
 *
 *
 * $Id$
 */
package org.mitre.spectrum;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Basic histogram class with one double per channel. Maximum precision 14 digits (check)
 *
 * @author Marc Colosimo
 */
public class TH1D {

    static public double DEFAULT_LOW = 0.0;
    static public double DEFAULT_UPPER = 1.0;
    private String fTitle = "Histogram";
    private String fXLabel = "Data";
    private int[] fBins;
    private int fNumBins;
    private int fUnderflows;
    private int fOverflows;
    private double fLow;
    private double fUp;
    private double fRange;

    /** The constructor will create an array of a given
     * number of bins. The range of the histogram given
     * by the upper and lower limit values.
     **/
    public TH1D(int numBins, double xlow, double xup) {
        // Check for bad range values.
        // Could throw an exception but will just
        // use default values;
        if (xup < xlow) {
            xlow = DEFAULT_LOW;
            xup = DEFAULT_UPPER;
        }
        if (numBins <= 0) {
            numBins = 1;
        }
        fNumBins = numBins;
        fBins = new int[fNumBins];
        fLow = xlow;
        fUp = xup;
        fRange = fUp - fLow;
    } // ctor

    // This constructor includes the title and horizontal
    // axis label.
    public TH1D(String title, String xLabel,
            int numBins, double xlow, double xup) {
        this(numBins, xlow, xup);// Invoke overloaded constructor
        this.fTitle = title;
        this.fXLabel = xLabel;
    } // ctor

    /** Get to title string. **/
    public String getTitle() {
        return fTitle;
    }

    /** Set the title. **/
    public void setTitle(String title) {
        fTitle = title;
    }

    /** Get to the horizontal axis label. **/
    public String getXLabel() {
        return fXLabel;
    }

    /** Set the horizontal axis label. **/
    public void setXLabel(String xLabel) {
        fXLabel = xLabel;
    }

    /** Get the low end of the range. **/
    public double getLower() {
        return fLow;
    }

    /** Get the high end of the range.**/
    public double getUpper() {
        return fUp;
    }

    public int getNumberOverflows() {
        return this.fOverflows;
    }

    public int getNumberUnderflows() {
        return this.fUnderflows;
    }

    /** Get the number of entries in the largest bin. **/
    public int getMax() {
        int max = 0;
        for (int i = 0; i < fNumBins; i++) {
            if (max < fBins[i]) {
                max = fBins[i];
            }
        }
        return max;
    }

    /** Get the number of entries in the smallest bin.**/
    public int getMin() {
        int min = this.getMax();
        for (int i = 0; i < fNumBins; i++) {
            if (min > fBins[i]) {
                min = fBins[i];
            }
        }
        return min;
    }

    /**
     * This method returns a reference to the fBins.
     * Note that this means the values of the histogram
     * could be altered by the caller object.
     **/
    public int[] getBins() {
        return fBins;
    }

    /**
     * Returns the total number of entries not counting
     * overflows and underflows.
     **/
    public int getTotal() {
        int total = 0;
        for (int i = 0; i < fNumBins; i++) {
            total += fBins[i];
        }
        return total;
    }

    /**
     * Add an entry to a bin.
     * @param x double value added if it is in the range:
     *   lower <= x < upper
     * @return the bin or -1 if overflow or underflow
     **/
    public int add(double x) {
        if (x >= this.fUp) {
            fOverflows++;
            return -1;
        } else if (x < this.fLow) {
            fUnderflows++;
            return -1;
        } else {
            final double val = x - fLow;
            int bin = (int) (fNumBins * (val / fRange)); // Casting to int will round off to lower integer value
            fBins[bin]++;   // Increment the corresponding bin
            return bin;
        }
    }

    //public void setBinContent(int bin, double x) {}
    /** Clear the histogram bins and the over and under flows.**/
    public void clear() {
        for (int i = 0; i < fNumBins; i++) {
            fBins[i] = 0;
            fOverflows = 0;
            fUnderflows = 0;
        }
    }

    /**
     * Provide access to the value in the bin element
     * specified by bin.
     *
     * @return the content, otherwise underflows if bin value negative or overflows
     *         if bin value more than the number of bins.
     **/
    public int getBinContent(int bin) {
        if (bin < 0) {
            return fUnderflows;
        } else if (bin >= fNumBins) {
            return fOverflows;
        } else {
            return fBins[bin];
        }
    }

    /**
     * Returns the center of the given bin element.
     */
    public double getBinCenter(final int bin) throws IndexOutOfBoundsException {
       if (bin < 0 ||bin >= this.fBins.length ) {
           throw new IndexOutOfBoundsException("Bin value out of range");
       }
       final double bin_width = this.fRange / this.fNumBins;
       return (bin - 0.5) * bin_width + this.fLow;
        /*
        final double lowerEdge = this.getBinLowEdge(bin);
        final double upperEdge = this.getBinLowEdge(bin + 1);  // fix for upper bounds
        return (lowerEdge + upperEdge) / 2;
         *
         */
    }

    /**
     * Returns the lower edge of the given bin element.
     * If the bin number is less than 0,then the Lower is returned.
     * If the bin number is greated than the number of bins, then the Upper is returned.
     */
    public double getBinLowEdge(final int bin) {
       if (bin <= 0) {
           return this.fLow;
       } else if (bin >= this.fBins.length) {
           return this.fUp;  // the lowerEdge of everything above.
       }
       final double bin_width = this.fRange / this.fNumBins;
       return bin * bin_width;
         //return bin * (this.fRange / this.fNumBins);  // fix for upper bounds
    }
    
    /**
     * Returns the bin that x (lower <= x < upper) belongs to or -1 if this is outside the limits.
     */
    public int findBin(final double x) {
        if (x >= this.fUp) {
            return -1;
        } else if (x < this.fLow) {
            return -1;
        }
        return (int) (this.fNumBins * 
                ( (x - this.fLow) / this.fRange) ); // Casting to int will round off to lower integer value
    }
    /**
     * Returns the average and standard deviation of the distribution of entries.
     **/
    public double[] getStats() {
        int total = 0;

        double wt_total = 0;
        double wt_total2 = 0;
        double[] stat = new double[2];
        final double bin_width = fRange / fNumBins;

        for (int i = 0; i < fNumBins; i++) {
            total += fBins[i];

            double bin_mid = (i - 0.5) * bin_width + fLow;
            wt_total += fBins[i] * bin_mid;
            wt_total2 += fBins[i] * bin_mid * bin_mid;
        }

        if (total > 0) {
            stat[0] = wt_total / total;
            double av2 = wt_total2 / total;
            stat[1] = Math.sqrt(av2 - stat[0] * stat[0]);
        } else {
            stat[0] = 0.0;
            stat[1] = -1.0;
        }

        return stat;
    }// getStats()

    /**
     * Create the histogram from a user derived array along with the
     * under and overflow values.<br>
     * The low and high range values that the histogram
     * corresponds to must be in passed as well.<br>
     *
     * @param userBins array of int values.
     * @param under number of underflows.
     * @param over number of overflows.
     * @param xlow value of the lower range limit.
     * @param xup value of the upper range limit.
     **/
    public void pack(int[] user_bins,
            int under, int over,
            double xlow, double xup) {
        fNumBins = user_bins.length;
        fBins = new int[fNumBins];
        for (int i = 0; i < fNumBins; i++) {
            fBins[i] = user_bins[i];
        }

        fLow = xlow;
        fUp = xup;
        fRange = fUp - fLow;
        fUnderflows = under;
        fOverflows = over;
    }// pack
    
    /**
     * Packs a distribution into this histogram
     * 
     * This does not clear it first.
     */
    public void pack(Map<Double, Integer> userBins) {
        for (Iterator<Entry<Double, Integer>> iter = userBins.entrySet().iterator(); iter.hasNext();) {
            Entry<Double, Integer> entry = iter.next();
            final double x = entry.getKey();
            if (x >= this.fUp) {
                this.fOverflows++;
            } else if (x < fLow) {
                this.fUnderflows++;
            } else {
                this.fBins[this.findBin(x)] += entry.getValue();
            }
        }
    }
}

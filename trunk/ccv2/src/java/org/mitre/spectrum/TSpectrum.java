/**
 * Created on 22 December 2009
 *
 * Based on Root. Copyright (C) 1995-2006, Rene Brun and Fons Rademakers.               *
 * All rights reserved.
 * GPL
 *
 * $Id$
 */
package org.mitre.spectrum;

import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Spectra Processing Class based on Root's TSpectrum class
 * <br>
 * This class contains advanced spectra processing functions for:
 * <ul>
 *  <li> One-dimensional background estimation
 *  <li> One-dimensional smoothing
 *  <li> One-dimensional deconvolution
 *  <li> One-dimensional peak search
 * </ul>
 * 
 * The algorithms in this class have been published in the following references:
<ol>
<li> M.Morhac et al.: Background elimination methods for
multidimensional coincidence gamma-ray spectra. Nuclear
Instruments and Methods in Physics Research A 401 (1997) 113-132.
<li> M.Morhac et al.: Efficient one- and two-dimensional Gold
deconvolution and its application to gamma-ray spectra
decomposition. Nuclear Instruments and Methods in Physics
Research A 401 (1997) 385-408.
<li> M.Morhac et al.: Identification of peaks in multidimensional
coincidence gamma-ray spectra. Nuclear Instruments and Methods in
Research Physics A  443(2000), 108-125.
</ol>
These NIM papers are also available as doc or ps files from:
<ul>
<li> <A href="ftp://root.cern.ch/root/Spectrum.doc">Spectrum.doc</A><br>
<li> <A href="ftp://root.cern.ch/root/SpectrumDec.ps.gz">SpectrumDec.ps.gz</A><br>
<li> <A href="ftp://root.cern.ch/root/SpectrumSrc.ps.gz">SpectrumSrc.ps.gz</A><br>
<li> <A href="ftp://root.cern.ch/root/SpectrumBck.ps.gz">SpectrumBck.ps.gz</A><br>
</ul>
 *
 * Java port based on Miroslav Morhac C++ version from Root.
 *
 * @link http://root.cern.ch/root/htmldoc/TSpectrum.html
 *
 * @author Miroslav Morhac
 * @author Marc Colosimo
 */
public class TSpectrum {

    private static final Log LOG = LogFactory.getLog(TSpectrum.class);
    public static final int PEAK_WINDOW = 1024;
    public static final int DEAFULT_NUM_PEAKS = 100;
    public static final int DEFAULT_RESOLUTION = 1;
    // static types - where enums
    static final int kBackOrder2 = 0;
    static final int kBackOrder4 = 1;
    static final int kBackOrder6 = 2;
    static final int kBackOrder8 = 3;
    static final int kBackIncreasingWindow = 0;
    static final int kBackDecreasingWindow = 1;
    static final int kBackSmoothing3 = 3;
    static final int kBackSmoothing5 = 5;
    static final int kBackSmoothing7 = 7;
    static final int kBackSmoothing9 = 9;
    static final int kBackSmoothing11 = 11;
    static final int kBackSmoothing13 = 13;
    static final int kBackSmoothing15 = 15;
    private int fMaxPeaks;                  // Maximum number of peaks to be found
    private int fNPeaks;                    // Number of peaks found
    private double[] fPosition;             // [fNPeaks] array of current peak positions
    private double[] fPositionX;            // [fNPeaks] X position of peaks
    private double[] fPositionY;            // [fNPeaks] Y position of peaks
    private double fResolution;           // Resolution of the neighboring peaks

    /*
     *    TH1          *fHistogram;      //resulting histogram
     */
    //private ArrayList<Double> fHistogram;
    //private TH1D fHistogram;                // resulting hisogram with one double per channel
    //make defaults for these
    private int fgAverageWindow = 3;       // Average window of searched peaks for Markov (default=3) (was static)
    private int fgIterations = 3;          // Maximum number of decon iterations (default=3) (was static)

    /**
     * Constructs a new Miroslav Morhac peak finder using the deafult number of peaks and resolution.
     */
    public TSpectrum() {
        this(DEAFULT_NUM_PEAKS, DEFAULT_RESOLUTION);
    }

    public TSpectrum(int maxPeaks) {
        this(maxPeaks, DEFAULT_RESOLUTION);
    }

    /**
     *
     * @param maxPeaks
     * @param resolution
     */
    public TSpectrum(int maxPeaks, double resolution) {

        if (maxPeaks <= 0) {
            maxPeaks = 1;
        }
        this.fMaxPeaks = maxPeaks;
        this.fPosition = new double[maxPeaks];
        this.fPositionX = new double[maxPeaks];
        this.fPositionY = new double[maxPeaks];
        //this.fHistogram = null;
        this.fNPeaks = 0;
        this.setResolution(resolution);
    }

    /**
     * Sets the resolution of the neighboring peaks
     * default value is 1 correspond to 3 sigma distance
     * between peaks. Higher values allow higher resolution
     * (smaller distance between peaks)
     */
    public void setResolution(double resolution) {
        /*
         * resolution: determines
        May be set later through SetResolution.
        End_Html */

        if (resolution > 1) {
            this.fResolution = resolution;
        } else {
            this.fResolution = DEFAULT_RESOLUTION;
        }
    }

    public double[] getPostionX() {
        return this.fPositionX;
    }

    public double[] getPositionY() {
        return this.fPositionY;
    }

    /**
     * Onedimensional peak search function using default parameters
     * <P>
     * sigma = 1
     * threshold=0.05
     * background and markov
     * <p>
     * @param hin histogram of source spectrum
     * @return the number of peaks
     */
    public int search(TH1D hin) throws Exception {
        return this.search(hin, 1, 0.05, null);
    }

    /**
     * One-dimensional peak search function
     * <p>
     * This function searches for peaks in source spectrum in hin
     * The number of found peaks and their positions are written into
     * the members fNpeaks and fPositionX.
     * The search is performed in the current histogram range.
     * <p>
     * By default, the background is removed before deconvolution.
     * Specify the option "nobackground" to not remove the background.
     * <p>
     * By default the "Markov" chain algorithm is used.
     * Specify the option "noMarkov" to disable this algorithm
     * Note that by default the source spectrum is replaced by a new spectrum
     *
     * @parma hin histogram of source spectrum
     * @parma sigma sigma of searched peaks, for details refer to the manual. 1 <= sigma <= 8
     * @parma threshold  peaks with amplitude less than threshold*highest_peak are discarded.  0<threshold<1
     * @param options nobackground (default on), nomarkov (deafult on)
     */
    public int search(TH1D hin, double sigma, double threshold, Set<String> options) throws Exception {
        if (hin == null || hin.getBins().length == 0) {
            LOG.fatal("Search: Either no histogram or no bins");
            throw new Exception();
        }
        final int dimension = 1;
        /*Int_t dimension = hin->GetDimension();
        if (dimension > 2) {
        Error("Search", "Only implemented for 1-d and 2-d histograms");
        return 0;
        }*/
        if (threshold <= 0 || threshold >= 1) {
            LOG.warn("Search: threshold must 0<threshold<1, threshold=0.05 assumed!");
            threshold = 0.05;
        }
        boolean background = true;
        boolean markov = true;
        if (options != null) {
            if (options.contains("nobackground")) {
                background = false;
                options.remove("nobackground");
            }
            if (options.contains("nomarkov")) {
                markov = false;
                options.remove("nomarkov");
            }
        }

        if (dimension == 1) {
            int first = 0;      // we assume this it a dense list, but check to see what hin->GetXaxis()->getFirst() does
            int last = hin.getBins().length;
            int size = last - first + 1;
            int bin, npeaks;
            final double[] source = new double[size];
            final double[] dest = new double[size];
            for (int i = 0; i < size; i++) {
                source[i] = hin.getBinContent(i + first); //hin.get(i + first);
            }
            if (sigma <= 1) {
                sigma = size / this.fMaxPeaks;
                if (sigma < 1) {
                    sigma = 1;
                }
                if (sigma > 8) {
                    sigma = 8;
                }
            }

            npeaks = this.searchHighRes(source, dest, size, sigma, 100 * threshold,
                    background, this.fgIterations, markov, this.fgAverageWindow);

            for (int i = 0; i < npeaks; i++) {
                bin = first + (int) (this.fPositionX[i] + 0.5);
                this.fPositionX[i] = hin.getBinCenter(bin);  // Ack in TH1.h  virtual Double_t GetBinCenter(Int_t bin) fXaxis.GetBinCenter(bin)
                this.fPositionY[i] = hin.getBinContent(bin); //  virtual Double_t GetBinContent(Int_t bin) const;
            }
            return npeaks;
        } else {
            LOG.warn("search: Only handles one dimension");
        }

        return 0;
    }

    /**
     * One-dimensional high-resolution peak search function</b>
    <p>
    This function searches for peaks in source spectrum. It is based on
    deconvolution method. First the background is removed (if desired), then
    Markov smoothed spectrum is calculated (if desired), the response
    function is generated according to given sigma, and then deconvolution is
    carried out. The order of peaks is arranged according to their heights in
    the spectrum after background elimination. The highest peak is the first in
    the list. On success it returns number of found peaks.
    <p>
    <b>Function parameters:</b>
    <ul>
    <li> source: pointer to the vector of source spectrum.
    <li> destVector: pointer to the vector of resulting deconvolved spectrum.
    <li> ssize: length of source spectrum.
    <li> sigma: sigma of searched peaks, for details we refer to manual.
    <li> threshold: threshold value in % for selected peaks, peaks with
    amplitude less than threshold*highest_peak/100
    are ignored, see manual.
    <li> backgroundRemove: logical variable, set if the removal of
    background before deconvolution is desired.
    <li> deconIterations-number of iterations in deconvolution operation.
    <li> markov: logical variable, if it is true, first the source spectrum
    is replaced by new spectrum calculated using Markov
    chains method.
    <li> averWindow: averanging window of searched peaks, for details
    we refer to manual (applies only for Markov method).
    </ul>
    <p>
    <b>Peaks searching:</b>
    <p>
    The goal of this function is to identify automatically the peaks in spectrum
    with the presence of the continuous background and statistical
    fluctuations - noise.
    <p>
    The common problems connected with correct peak identification are:
    <ul>
    <li> non-sensitivity to noise, i.e., only statistically
    relevant peaks should be identified.
    <li> non-sensitivity of the algorithm to continuous
    background.
    <li> ability to identify peaks close to the edges of the
    spectrum region. Usually peak finders fail to detect them.
    <li> resolution, decomposition of doublets and multiplets.
    The algorithm should be able to recognize close positioned peaks.
    <li> ability to identify peaks with different sigma.
    </ul>
    <img width=600 height=375 src="gif/TSpectrum_Searching1.jpg">
    <p>
    Fig. 27 An example of one-dimensional synthetic spectrum with found peaks
    denoted by markers.
    <p>
    <b>References:</b>
    <ol>
    <li> M.A. Mariscotti: A method for identification of peaks in the presence of
    background and its application to spectrum analysis. NIM 50 (1967),
    309-320.
    <li> M. Morhá&#269;, J. Kliman, V.  Matouek, M. Veselský,
    I. Turzo.:Identification of peaks in
    multidimensional coincidence gamma-ray spectra. NIM, A443 (2000) 108-125.
    <li> Z.K. Silagadze, A new algorithm for automatic photopeak searches. NIM
    A 376 (1996), 451.
     * </ol>
     * 
     * @param sourceVector
     * @param destVector
     * @param sourceSize
     * @param sigma
     * @param threshold
     * @param backgroundRemove
     * @param deconIterations
     * @param markov
     * @param averageWindow
     * @return number of peaks found
     */
    public int searchHighRes(final double[] sourceVector, double[] destVector, int sourceSize,
            double sigma, double threshold, boolean backgroundRemove, int deconIterations,
            boolean markov, int averageWindow) throws Exception {

        int i, j;
        final int numberIterations = (int) (7 * sigma + 0.5);
        double a, b, c;
        int k, lindex, posit, imin, imax, jmin, jmax, lh_gold, priz;
        double lda, ldb, ldc, area, maximum, maximum_decon;
        int xmin, xmax, l, peak_index = 0;
        final int size_ext = sourceSize + 2 * numberIterations;
        final int shift = numberIterations;
        int bw = 2, w;
        double maxch;

        double nom, nip, nim, sp, sm, plocha = 0;
        double m0low = 0, m1low = 0, m2low = 0, l0low = 0, l1low = 0, detlow, av, men;

        // @TODO: these checks should throw an exception
        if (sigma < 1) {
            LOG.error("searchHighRes: Invalid sigma, must be greater than or equal to 1!");
            throw new Exception();
        }

        if (threshold <= 0 || threshold >= 100) {
            LOG.error("searchHighRes: Invalid threshold, must be positive and less than 100!");
            throw new Exception();
        }

        j = (int) (5.0 * sigma + 0.5);
        if (j >= PEAK_WINDOW / 2) {
            LOG.error("searchHighRes: Too large of a sigma!");
            throw new Exception();
        }

        if (markov) {
            if (averageWindow <= 0) {
                LOG.error(String.format("searchHighRes: Averange window (%d) must be positive!", averageWindow));
                return 0;
            }
        }

        if (backgroundRemove) {
            if (sourceSize < 2 * numberIterations + 1) {
                LOG.error("searchHighRes: Too large clipping window");
                return 0;
            }
        }

        k = (int) (2 * sigma + 0.5);
        if (k >= 2) {
            for (i = 0; i < k; i++) {
                a = i;
                b = sourceVector[i];
                m0low += 1;
                m1low += a;
                m2low += a * a;
                l0low += b;
                l1low += a * b;
            }
            detlow = m0low * m2low - m1low * m1low;
            if (detlow != 0) {
                l1low = (-l0low * m1low + l1low * m0low) / detlow;
            } else {
                l1low = 0;
            }
            if (l1low > 0) {
                l1low = 0;
            }
        } else {
            // is this correct
            l1low = 0;
        }

        i = (int) (7 * sigma + 0.5);
        i = 2 * i;
        final double[] working_space = new double[7 * (sourceSize + i)];
        // for (j = 0;j<7 * (sourceSize + i); j++) working_space[j] = 0;
        for (i = 0; i < size_ext; i++) {
            if (i < shift) {
                a = i - shift;
                working_space[i + size_ext] = sourceVector[0] + l1low * a;
                if (working_space[i + size_ext] < 0) {
                    working_space[i + size_ext] = 0;
                }
            } else if (i >= sourceSize + shift) {
                a = i - (sourceSize - 1 + shift);
                working_space[i + size_ext] = sourceVector[sourceSize - 1];
                if (working_space[i + size_ext] < 0) {
                    working_space[i + size_ext] = 0;
                }
            } else {
                working_space[i + size_ext] = sourceVector[i - shift];
            }
        }

        // more tricky bit of code
        if (backgroundRemove) {
            LOG.info("searchHighRes: Removing background.");
            for (i = 1; i <= numberIterations; i++) {
                for (j = i; j < size_ext - i; j++) {
                    if (markov == false) {
                        a = working_space[size_ext + j];
                        b = (working_space[size_ext + j - i] + working_space[size_ext + j + i]) / 2.0;
                        if (b < a) {
                            a = b;
                        }
                        working_space[j] = a;
                    } else {
                        a = working_space[size_ext + j];
                        av = 0;
                        men = 0;
                        for (w = j - bw; w <= j + bw; w++) {
                            if (w >= 0 && w < size_ext) {
                                av += working_space[size_ext + w];
                                men += 1;
                            }
                        }
                        av = av / men;
                        b = 0;
                        men = 0;
                        for (w = j - i - bw; w <= j - i + bw; w++) {
                            if (w >= 0 && w < size_ext) {
                                b += working_space[size_ext + w];
                                men += 1;
                            }
                        }
                        b = b / men;
                        c = 0;
                        men = 0;
                        for (w = j + i - bw; w <= j + i + bw; w++) {
                            if (w >= 0 && w < size_ext) {
                                c += working_space[size_ext + w];
                                men += 1;
                            }
                        }
                        c = c / men;
                        b = (b + c) / 2;
                        if (b < a) {
                            av = b;
                        }
                        working_space[j] = av;
                    }
                }
                for (j = i; j < size_ext - i; j++) {
                    working_space[size_ext + j] = working_space[j];
                }
            }
            for (j = 0; j < size_ext; j++) {
                if (j < shift) {
                    a = j - shift;
                    b = sourceVector[0] + l1low * a;
                    if (b < 0) {
                        b = 0;
                    }
                    working_space[size_ext + j] = b - working_space[size_ext + j];
                } else if (j >= sourceSize + shift) {
                    a = j - (sourceSize - 1 + shift);
                    b = sourceVector[sourceSize - 1];
                    if (b < 0) {
                        b = 0;
                    }
                    working_space[size_ext + j] = b - working_space[size_ext + j];
                } else {
                    working_space[size_ext + j] = sourceVector[j - shift] - working_space[size_ext + j];
                }
            }
            for (j = 0; j < size_ext; j++) {
                if (working_space[size_ext + j] < 0) {
                    working_space[size_ext + j] = 0;
                }
            }
        }

        // what does this do?
        for (i = 0; i < size_ext; i++) {
            working_space[i + 6 * size_ext] = working_space[i + size_ext];
        }

        if (markov) {
            LOG.info("searchHighRes: Making new spectrum calculated from orignal using Markov chains method");
            for (j = 0; j < size_ext; j++) {
                working_space[2 * size_ext + j] = working_space[size_ext + j];
            }
            xmin = 0;
            xmax = size_ext - 1;
            for (i = 0, maxch = 0; i < size_ext; i++) {
                working_space[i] = 0;
                if (maxch < working_space[2 * size_ext + i]) {
                    maxch = working_space[2 * size_ext + i];
                }
                plocha += working_space[2 * size_ext + i];
            }
            if (maxch == 0) {
                return 0;
            }
            nom = 1;
            working_space[xmin] = 1;
            for (i = xmin; i < xmax; i++) {
                nip = working_space[2 * size_ext + i] / maxch;
                nim = working_space[2 * size_ext + i + 1] / maxch;
                sp = 0;
                sm = 0;
                for (l = 1; l <= averageWindow; l++) {
                    if ((i + l) > xmax) {
                        a = working_space[2 * size_ext + xmax] / maxch;
                    } else {
                        a = working_space[2 * size_ext + i + l] / maxch;
                    }

                    b = a - nip;
                    if (a + nip <= 0) {
                        a = 1;
                    } else {
                        a = Math.sqrt(a + nip);
                    }

                    b = b / a;
                    b = Math.exp(b);
                    sp = sp + b;
                    if ((i - l + 1) < xmin) {
                        a = working_space[2 * size_ext + xmin] / maxch;
                    } else {
                        a = working_space[2 * size_ext + i - l + 1] / maxch;
                    }

                    b = a - nim;
                    if (a + nim <= 0) {
                        a = 1;
                    } else {
                        a = Math.sqrt(a + nim);
                    }
                    b = b / a;
                    b = Math.exp(b);
                    sm = sm + b;
                }
                a = sp / sm;
                a = working_space[i + 1] = working_space[i] * a;
                nom = nom + a;
            }
            for (i = xmin; i <= xmax; i++) {
                working_space[i] = working_space[i] / nom;
            }
            for (j = 0; j < size_ext; j++) {
                working_space[size_ext + j] = working_space[j] * plocha;
            }
            for (j = 0; j < size_ext; j++) {
                working_space[2 * size_ext + j] = working_space[size_ext + j];
            }

            if (backgroundRemove) {
                for (i = 1; i <= numberIterations; i++) {
                    for (j = i; j < size_ext - i; j++) {
                        a = working_space[size_ext + j];
                        b = (working_space[size_ext + j - i] + working_space[size_ext + j + i]) / 2.0;
                        if (b < a) {
                            a = b;
                        }
                        working_space[j] = a;
                    }
                    for (j = i; j < size_ext - i; j++) {
                        working_space[size_ext + j] = working_space[j];
                    }
                }
                for (j = 0; j < size_ext; j++) {
                    working_space[size_ext + j] = working_space[2 * size_ext + j] - working_space[size_ext + j];
                }
            }
        } // endif markov

        /**
         * deconvolution starts
         */
        LOG.info("searchHighRes: Deconvoluting");
        area = 0;
        lh_gold = -1;
        posit = 0;
        maximum = 0;
        //generate response vector
        for (i = 0; i < size_ext; i++) {
            lda = (double) i - 3 * sigma;
            lda = lda * lda / (2 * sigma * sigma);
            j = (int) (1000 * Math.exp(-lda));
            lda = j;
            if (lda != 0) {
                lh_gold = i + 1;
            }

            working_space[i] = lda;
            area = area + lda;
            if (lda > maximum) {
                maximum = lda;
                posit = i;
            }
        }
        //read source vector
        for (i = 0; i < size_ext; i++) {
            working_space[2 * size_ext + i] = Math.abs(working_space[size_ext + i]);
        }
        //create matrix at*a(vector b)
        i = lh_gold - 1;
        if (i > size_ext) {
            i = size_ext;
        }

        imin = -i;
        imax = i;
        for (i = imin; i <= imax; i++) {
            lda = 0;
            jmin = 0;
            if (i < 0) {
                jmin = -i;
            }
            jmax = lh_gold - 1 - i;
            if (jmax > (lh_gold - 1)) {
                jmax = lh_gold - 1;
            }

            for (j = jmin; j <= jmax; j++) {
                ldb = working_space[j];
                ldc = working_space[i + j];
                lda = lda + ldb * ldc;
            }
            working_space[size_ext + i - imin] = lda;
        }
        //create vector p
        i = lh_gold - 1;
        imin = -i;          // be better if imin and imax were final
        imax = size_ext + i - 1;
        for (i = imin; i <= imax; i++) {
            lda = 0;
            for (j = 0; j <= (lh_gold - 1); j++) {
                ldb = working_space[j];
                k = i + j;
                if (k >= 0 && k < size_ext) {
                    ldc = working_space[2 * size_ext + k];
                    lda = lda + ldb * ldc;
                }

            }
            working_space[4 * size_ext + i - imin] = lda;
        }
        //move vector p
        for (i = imin; i <= imax; i++) {
            working_space[2 * size_ext + i - imin] = working_space[4 * size_ext + i - imin];
        }
        //initialization of resulting vector
        for (i = 0; i < size_ext; i++) {
            working_space[i] = 1;
        }
        //START OF ITERATIONS
        for (lindex = 0; lindex < deconIterations; lindex++) {
            for (i = 0; i < size_ext; i++) {
                if (Math.abs(working_space[2 * size_ext + i]) > 0.00001
                        && Math.abs(working_space[i]) > 0.00001) {
                    lda = 0;
                    jmin = lh_gold - 1;
                    if (jmin > i) {
                        jmin = i;
                    }

                    jmin = -jmin;
                    jmax = lh_gold - 1;
                    if (jmax > (size_ext - 1 - i)) {
                        jmax = size_ext - 1 - i;
                    }

                    for (j = jmin; j <= jmax; j++) {
                        ldb = working_space[j + lh_gold - 1 + size_ext];
                        ldc = working_space[i + j];
                        lda = lda + ldb * ldc;
                    }
                    ldb = working_space[2 * size_ext + i];
                    if (lda != 0) {
                        lda = ldb / lda;
                    } else {
                        lda = 0;
                    }

                    ldb = working_space[i];
                    lda = lda * ldb;
                    working_space[3 * size_ext + i] = lda;
                }
            }
            for (i = 0; i < size_ext; i++) {
                working_space[i] = working_space[3 * size_ext + i];
            }
        }
        //shift resulting spectrum
        for (i = 0; i < size_ext; i++) {
            lda = working_space[i];
            j = i + posit;
            j = j % size_ext;
            working_space[size_ext + j] = lda;
        }
        //write back resulting spectrum
        maximum = 0;
        maximum_decon = 0;
        j = lh_gold - 1;
        for (i = 0; i < size_ext - j; i++) {
            if (i >= shift && i < sourceSize + shift) {
                working_space[i] = area * working_space[size_ext + i + j];
                if (maximum_decon < working_space[i]) {
                    maximum_decon = working_space[i];
                }
                if (maximum < working_space[6 * size_ext + i]) {
                    maximum = working_space[6 * size_ext + i];
                }
            } else {
                working_space[i] = 0;
            }
        }
        lda = 1;
        if (lda > threshold) {
            lda = threshold;
        }
        lda = lda / 100;

        //searching for peaks in deconvolved spectrum
        for (i = 1; i < size_ext - 1; i++) {
            if (working_space[i] > working_space[i - 1] && working_space[i] > working_space[i + 1]) {
                if (i >= shift && i < sourceSize + shift) {
                    if (working_space[i] > lda * maximum_decon && working_space[6 * size_ext + i] > threshold * maximum / 100.0) {
                        for (j = i - 1, a = 0, b = 0; j <= i + 1; j++) {
                            a += (double) (j - shift) * working_space[j];
                            b += working_space[j];
                        }
                        a = a / b;
                        if (a < 0) {
                            a = 0;
                        }

                        if (a >= sourceSize) {
                            a = sourceSize - 1;
                        }
                        if (peak_index == 0) {
                            this.fPositionX[0] = a;
                            peak_index = 1;
                        } else {
                            for (j = 0, priz = 0; j < peak_index && priz == 0; j++) {
                                if (working_space[6 * size_ext + shift + (int) a] > working_space[6 * size_ext + shift + (int) this.fPositionX[j]]) {
                                    priz = 1;
                                }
                            }
                            if (priz == 0) {
                                if (j < this.fMaxPeaks) {
                                    this.fPositionX[j] = a;
                                }
                            } else {
                                for (k = peak_index; k >= j; k--) {
                                    if (k < this.fMaxPeaks) {
                                        this.fPositionX[k] = this.fPositionX[k - 1];
                                    }
                                }
                                this.fPositionX[j - 1] = a;
                            }
                            if (peak_index < this.fMaxPeaks) {
                                peak_index += 1;
                            }
                        }
                    }
                }
            }
        }

        // copy over to destVector
        for (i = 0; i < sourceSize; i++) {
            destVector[i] = working_space[i + shift];
        }
        fNPeaks = peak_index;
        if (peak_index == fMaxPeaks) {
            LOG.warn("searchHighRes: Peak buffer full");
        }
        return fNPeaks;
    }

    /**
     * One-dimensional deconvolution function
     *   <p>
    This function calculates deconvolution from source spectrum according to
    response spectrum using Gold deconvolution algorithm. The result is placed
    in the vector pointed by source pointer. On successful completion it
    returns 0. On error it returns pointer to the string describing error. If
    desired after every numberIterations one can apply boosting operation
    (exponential function with exponent given by boost coefficient) and repeat
    it numberRepetitions times.
    <p>
    Function parameters:
    <ul>
    <li>source:  pointer to the vector of source spectrum
    <li>response:     pointer to the vector of response spectrum
    <li>ssize:    length of source and response spectra
    numberIterations, for details we refer to the reference given below
    numberRepetitions, for repeated boosted deconvolution
    boost, boosting coefficient
    </ul>
    The goal of this function is the improvement of the resolution in spectra,
    decomposition of multiplets. The mathematical formulation of
    the convolution system is:
    <p>
    <img width=585 height=84 src="gif/TSpectrum_Deconvolution1.gif">
    <p>
    where h(i) is the impulse response function, x, y are input and output
    vectors, respectively, N is the length of x and h vectors. In matrix form
    we have:
    <p>
    <img width=597 height=360 src="gif/TSpectrum_Deconvolution2.gif">
    <p>
    Let us assume that we know the response and the output vector (spectrum) of
    the above given system. The deconvolution represents solution of the
    overdetermined system of linear equations, i.e., the calculation of the
    vector <b>x</b>. From numerical stability point of view the operation of
    deconvolution is extremely critical (ill-posed problem) as well as time
    consuming operation. The Gold deconvolution algorithm proves to work very
    well, other methods (Fourier, VanCittert etc) oscillate. It is suitable to
    process positive definite data (e.g. histograms).
    <p>
    <b>Gold deconvolution algorithm:</b>
    <p>
    <img width=551 height=233 src="gif/TSpectrum_Deconvolution3.gif">
    <p>
    Where L is given number of iterations (numberIterations parameter).
    <p>
    <b>Boosted deconvolution:</b>
    <ol>
    <li> Set the initial solution:
    End_Html Begin_Latex x^{(0)} = [1,1,...,1]^{T} End_Latex Begin_Html
    <li> Set required number of repetitions R and iterations L.
    <li> Set r = 1.
    <li>Using Gold deconvolution algorithm for k=1,2,...,L find
    End_Html Begin_Latex x^{(L)} End_Latex Begin_Html
    <li> If r = R stop calculation, else
    <ol>
    <li> Apply boosting operation, i.e., set
    End_Html Begin_Latex x^{(0)}(i) = [x^{(L)}(i)]^{p} End_Latex Begin_Html
    i=0,1,...N-1 and p is boosting coefficient &gt;0.
    <li> r = r + 1
    <li> continue in 4.
    </ol>
    </ol>
    <p>
    <b>References:</b>
    <ol>
    <li> Gold R., ANL-6984, Argonne National Laboratories, Argonne Ill, 1964.
    <li> Coote G.E., Iterative smoothing and deconvolution of one- and two-dimensional
    elemental distribution data, NIM B 130 (1997) 118.
    <li> M. Morhá&#269;, J. Kliman, V.  Matouek, M. Veselský,
    I. Turzo: Efficient one- and two-dimensional Gold deconvolution and
    its application to gamma-ray spectra decomposition. NIM, A401 (1997) 385-408.
    <li> Morhá&#269; M., Matouek V., Kliman J., Efficient algorithm of multidimensional
    deconvolution and its application to nuclear data processing, Digital Signal
    Processing 13 (2003) 144.
    </ol>
    <p>
    <i>Example 8 - script Deconvolution.c :</i>
    <p>
    response function (usually peak) should be shifted left to the first
    non-zero channel (bin) (see Figure 9)
    <p>
    <img width=600 height=340 src="gif/TSpectrum_Deconvolution1.jpg">
    <p>
    Figure 9 Response spectrum.
    <p>
    <img width=946 height=407 src="gif/TSpectrum_Deconvolution2.jpg">
    <p>
    Figure 10 Principle how the response matrix is composed inside of the
    Deconvolution function.
    <img width=601 height=407 src="gif/TSpectrum_Deconvolution3.jpg">
    <p>
    Figure 11 Example of Gold deconvolution. The original source spectrum is
    drawn with black color, the spectrum after the deconvolution (10000
    iterations) with red color.
    <p>
    Script:
    <p>
    <pre>
    // Example to illustrate deconvolution function (class TSpectrum).
    // To execute this example, do
    // root > .x Deconvolution.C

    #include <TSpectrum>

    void Deconvolution() {
    Int_t i;
    Double_t nbins = 256;
    Double_t xmin  = 0;
    Double_t xmax  = (Double_t)nbins;
    Float_t * source = new float[nbins];
    Float_t * response = new float[nbins];
    TH1F *h = new TH1F("h","Deconvolution",nbins,xmin,xmax);
    TH1F *d = new TH1F("d","",nbins,xmin,xmax);
    TFile *f = new TFile("spectra\\TSpectrum.root");
    h=(TH1F*) f->Get("decon1;1");
    TFile *fr = new TFile("spectra\\TSpectrum.root");
    d=(TH1F*) fr->Get("decon_response;1");
    for (i = 0; i < nbins; i++) source[i]=h->GetBinContent(i + 1);
    for (i = 0; i < nbins; i++) response[i]=d->GetBinContent(i + 1);
    TCanvas *Decon1 = gROOT->GetListOfCanvases()->FindObject("Decon1");
    if (!Decon1) Decon1 = new TCanvas("Decon1","Decon1",10,10,1000,700);
    h->Draw("L");
    TSpectrum *s = new TSpectrum();
    s->Deconvolution(source,response,256,1000,1,1);
    for (i = 0; i < nbins; i++) d->SetBinContent(i + 1,source[i]);
    d->SetLineColor(kRed);
    d->Draw("SAME L");
    }
    </pre>
    <p>
    <b>Examples of Gold deconvolution method:</b>
    <p>
    First let us study the influence of the number of iterations on the
    deconvolved spectrum (Figure 12).
    <p>
    <img width=602 height=409 src="gif/TSpectrum_Deconvolution_wide1.jpg">
    <p>
    Figure 12 Study of Gold deconvolution algorithm.The original source spectrum
    is drawn with black color, spectrum after 100 iterations with red color,
    spectrum after 1000 iterations with blue color, spectrum after 10000
    iterations with green color and spectrum after 100000 iterations with
    magenta color.
    <p>
    For relatively narrow peaks in the above given example the Gold
    deconvolution method is able to decompose overlapping peaks practically to
    delta - functions. In the next example we have chosen a synthetic data
    (spectrum, 256 channels) consisting of 5 very closely positioned, relatively
    wide peaks (sigma =5), with added noise (Figure 13). Thin lines represent
    pure Gaussians (see Table 1); thick line is a resulting spectrum with
    additive noise (10% of the amplitude of small peaks).
    <p>
    <img width=600 height=367 src="gif/TSpectrum_Deconvolution_wide2.jpg">
    <p>
    Figure 13 Testing example of synthetic spectrum composed of 5 Gaussians with
    added noise.
    <p>
    <table border=solid><tr>
    <td> Peak # </td><td> Position </td><td> Height </td><td> Area   </td>
    </tr><tr>
    <td> 1      </td><td> 50       </td><td> 500    </td><td> 10159  </td>
    </tr><tr>
    <td> 2      </td><td> 70       </td><td> 3000   </td><td> 60957  </td>
    </tr><tr>
    <td> 3      </td><td> 80       </td><td> 1000   </td><td> 20319  </td>
    </tr><tr>
    <td> 4      </td><td> 100      </td><td> 5000   </td><td> 101596 </td>
    </tr><tr>
    <td> 5      </td><td> 110      </td><td> 500    </td><td> 10159  </td>
    </tr></table>
    <p>
    Table 1 Positions, heights and areas of peaks in the spectrum shown in
    Figure 13.
    <p>
    In ideal case, we should obtain the result given in Figure 14. The areas of
    the Gaussian components of the spectrum are concentrated completely to
    delta-functions. When solving the overdetermined system of linear equations
    with data from Figure 13 in the sense of minimum least squares criterion
    without any regularization we obtain the result with large oscillations
    (Figure 15). From mathematical point of view, it is the optimal solution in
    the unconstrained space of independent variables. From physical point of
    view we are interested only in a meaningful solution. Therefore, we have to
    employ regularization techniques (e.g. Gold deconvolution) and/or to
    confine the space of allowed solutions to subspace of positive solutions.
    <p>
    <img width=589 height=189 src="gif/TSpectrum_Deconvolution_wide3.jpg">
    <p>
    Figure 14 The same spectrum like in Figure 13, outlined bars show the
    contents of present components (peaks).
    <img width=585 height=183 src="gif/TSpectrum_Deconvolution_wide4.jpg">
    <p>
    Figure 15 Least squares solution of the system of linear equations without
    regularization.
    <p>
    <i>Example 9 - script Deconvolution_wide.c</i>
    <p>
    When we employ Gold deconvolution algorithm we obtain the result given in
    Fig. 16. One can observe that the resulting spectrum is smooth. On the
    other hand the method is not able to decompose completely the peaks in the
    spectrum.
    <p>
    <img width=601 height=407 src="gif/TSpectrum_Deconvolution_wide5.jpg">
    Figure 16 Example of Gold deconvolution for closely positioned wide peaks.
    The original source spectrum is drawn with black color, the spectrum after
    the deconvolution (10000 iterations) with red color.
    <p>
    Script:
    <p>
    <pre>
    // Example to illustrate deconvolution function (class TSpectrum).
    // To execute this example, do
    // root > .x Deconvolution_wide.C

    #include <TSpectrum>

    void Deconvolution_wide() {
    Int_t i;
    Double_t nbins = 256;
    Double_t xmin  = 0;
    Double_t xmax  = (Double_t)nbins;
    Float_t * source = new float[nbins];
    Float_t * response = new float[nbins];
    TH1F *h = new TH1F("h","Deconvolution",nbins,xmin,xmax);
    TH1F *d = new TH1F("d","",nbins,xmin,xmax);
    TFile *f = new TFile("spectra\\TSpectrum.root");
    h=(TH1F*) f->Get("decon3;1");
    TFile *fr = new TFile("spectra\\TSpectrum.root");
    d=(TH1F*) fr->Get("decon_response_wide;1");
    for (i = 0; i < nbins; i++) source[i]=h->GetBinContent(i + 1);
    for (i = 0; i < nbins; i++) response[i]=d->GetBinContent(i + 1);
    TCanvas *Decon1 = gROOT->GetListOfCanvases()->FindObject("Decon1");
    if (!Decon1) Decon1 = new TCanvas("Decon1",
    "Deconvolution of closely positioned overlapping peaks using Gold deconvolution method",10,10,1000,700);
    h->SetMaximum(30000);
    h->Draw("L");
    TSpectrum *s = new TSpectrum();
    s->Deconvolution(source,response,256,10000,1,1);
    for (i = 0; i < nbins; i++) d->SetBinContent(i + 1,source[i]);
    d->SetLineColor(kRed);
    d->Draw("SAME L");
    }
    </pre>
    <p>
    <i>Example 10 - script Deconvolution_wide_boost.c :</i>
    <p>
    Further let us employ boosting operation into deconvolution (Fig. 17).
    <p>
    <img width=601 height=407 src="gif/TSpectrum_Deconvolution_wide6.jpg">
    <p>
    Figure 17 The original source spectrum is drawn with black color, the
    spectrum after the deconvolution with red color. Number of iterations = 200,
    number of repetitions = 50 and boosting coefficient = 1.2.
    <p>
    <table border=solid><tr>
    <td> Peak # </td> <td> Original/Estimated (max) position </td> <td> Original/Estimated area </td>
    </tr> <tr>
    <td> 1 </td> <td> 50/49 </td> <td> 10159/10419 </td>
    </tr> <tr>
    <td> 2 </td> <td> 70/70 </td> <td> 60957/58933 </td>
    </tr> <tr>
    <td> 3 </td> <td> 80/79 </td> <td> 20319/19935 </td>
    </tr> <tr>
    <td> 4 </td> <td> 100/100 </td> <td> 101596/105413 </td>
    </tr> <tr>
    <td> 5 </td> <td> 110/117 </td> <td> 10159/6676 </td>
    </tr> </table>
    <p>
    Table 2 Results of the estimation of peaks in spectrum shown in Figure 17.
    <p>
    One can observe that peaks are decomposed practically to delta functions.
    Number of peaks is correct, positions of big peaks as well as their areas
    are relatively well estimated. However there is a considerable error in
    the estimation of the position of small right hand peak.
    <p>
    Script:
    <p>
    <pre>
    // Example to illustrate deconvolution function (class TSpectrum).
    // To execute this example, do
    // root > .x Deconvolution_wide_boost.C

    #include <TSpectrum>

    void Deconvolution_wide_boost() {
    Int_t i;
    Double_t nbins = 256;
    Double_t xmin  = 0;
    Double_t xmax  = (Double_t)nbins;
    Float_t * source = new float[nbins];
    Float_t * response = new float[nbins];
    TH1F *h = new TH1F("h","Deconvolution",nbins,xmin,xmax);
    TH1F *d = new TH1F("d","",nbins,xmin,xmax);
    TFile *f = new TFile("spectra\\TSpectrum.root");
    h=(TH1F*) f->Get("decon3;1");
    TFile *fr = new TFile("spectra\\TSpectrum.root");
    d=(TH1F*) fr->Get("decon_response_wide;1");
    for (i = 0; i < nbins; i++) source[i]=h->GetBinContent(i + 1);
    for (i = 0; i < nbins; i++) response[i]=d->GetBinContent(i + 1);
    TCanvas *Decon1 = gROOT->GetListOfCanvases()->FindObject("Decon1");
    if (!Decon1) Decon1 = new TCanvas("Decon1",
    "Deconvolution of closely positioned overlapping peaks using boosted Gold deconvolution method",10,10,1000,700);
    h->SetMaximum(110000);
    h->Draw("L");
    TSpectrum *s = new TSpectrum();
    s->Deconvolution(source,response,256,200,50,1.2);
    for (i = 0; i < nbins; i++) d->SetBinContent(i + 1,source[i]);
    d->SetLineColor(kRed);
    d->Draw("SAME L");
    }
    </pre>
     */
    public String deconvolution(double[] source, final double[] response, int numberIterations,
            int numberRepetitions, double boost) throws Exception {

        if (source.length != response.length) {
            return "Source and response vectors are not the same size!";
        }
        final int ssize = source.length;
        if (ssize == 0) {
            return "Source and response are empty";
        }
        if (numberRepetitions <= 0) {
            return "Wrong parameter size for numberRepetitions (<= 0)!";
        }

        /*
         * Working_space-pointer to the working vector
         *(its size must be 4*ssize of source spectrum)
         */
        final double working_space[] = new double[4 * ssize];
        int i, j, k, lindex, posit, lh_gold, l, repet;
        double lda, ldb, ldc, area, maximum;

        area = 0.0;
        lh_gold = -1;
        posit = 0;
        maximum = 0;

        // read response vector
        for (i = 0; i < ssize; i++) {
            lda = response[i];
            if (lda != 0) {
                lh_gold = i + 1;
            }
            working_space[i] = lda;
            area += lda;
            if (lda > maximum) {
                maximum = lda;
                posit = i;
            }
        }
        if (lh_gold == -1) {
            return "ZERO RESPONSE VECTOR";
        }

        //read source vector
        for (i = 0; i < ssize; i++) {
            working_space[2 * ssize + i] = source[i];
        }

        // create matrix at*a and vector at*y
        for (i = 0; i < ssize; i++) {
            lda = 0;
            for (j = 0; j < ssize; j++) {
                ldb = working_space[j];
                k = i + j;
                if (k < ssize) {
                    ldc = working_space[k];
                    lda = lda + ldb * ldc;
                }
            }
            working_space[ssize + i] = lda;
            lda = 0;
            for (k = 0; k < ssize; k++) {
                l = k - i;
                if (l >= 0) {
                    ldb = working_space[l];
                    ldc = working_space[2 * ssize + k];
                    lda = lda + ldb * ldc;
                }
            }
            working_space[3 * ssize + i] = lda;
        }

        // move vector at*y
        for (i = 0; i < ssize; i++) {
            // would memcopy work here? this will not be very fast but it isn't very large
            working_space[2 * ssize + i] = working_space[3 * ssize + i];
        }

        //initialization of resulting vector
        for (i = 0; i < ssize; i++) {
            working_space[i] = 1;
        }

        //**START OF ITERATIONS**
        for (repet = 0; repet < numberRepetitions; repet++) {
            if (repet != 0) {
                for (i = 0; i < ssize; i++) {
                    working_space[i] = Math.pow(working_space[i], boost);
                }
            }
            for (lindex = 0; lindex < numberIterations; lindex++) {
                for (i = 0; i < ssize; i++) {
                    if (working_space[2 * ssize + i] > 0.000001
                            && working_space[i] > 0.000001) {
                        lda = 0;
                        for (j = 0; j < lh_gold; j++) {
                            ldb = working_space[j + ssize];
                            if (j != 0) {
                                k = i + j;
                                ldc = 0;
                                if (k < ssize) {
                                    ldc = working_space[k];
                                }
                                k = i - j;
                                if (k >= 0) {
                                    ldc += working_space[k];
                                }
                            } else {
                                ldc = working_space[i];
                            }
                            lda = lda + ldb * ldc;
                        }
                        ldb = working_space[2 * ssize + i];
                        if (lda != 0) {
                            lda = ldb / lda;
                        } else {
                            lda = 0;
                        }
                        ldb = working_space[i];
                        lda = lda * ldb;
                        working_space[3 * ssize + i] = lda;
                    }
                }
                for (i = 0; i < ssize; i++) {
                    working_space[i] = working_space[3 * ssize + i];
                }
            }
        }

        //shift resulting spectrum
        for (i = 0; i < ssize; i++) {
            lda = working_space[i];
            j = i + posit;
            j = j % ssize;
            working_space[ssize + j] = lda;
        }

        //write back resulting spectrum
        for (i = 0; i < ssize; i++) {
            source[i] = area * working_space[ssize + i];
        }

        return null;  // no error
    }
}

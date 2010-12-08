/**
 * Created on Nov 23, 2007
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
package org.mitre.ccv;

import java.util.Properties;
import java.util.NoSuchElementException;
import java.util.ResourceBundle;
import java.util.MissingResourceException;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.PrintWriter;

import java.sql.SQLException;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.log4j.Level;

import org.apache.log4j.Logger;
import org.apache.xml.serialize.XMLSerializer;
import org.apache.xml.serialize.OutputFormat;

import org.mitre.ccv.index.IndexedCompleteCompositionVectorSet;
import org.mitre.ccv.sql.AbstractVectorSetSQL;
import org.mitre.ccv.sql.EmbeddedVectorSetSQL;
import org.mitre.ccv.sql.VectorSetSQL;

import org.mitre.bio.Sequence;
import org.mitre.bio.io.FastaIterator;

import org.mitre.bio.phylo.DistanceMatrix;
import org.mitre.bio.phylo.dom.Forest;

import org.mitre.bio.phylo.tree.NeighborJoiningTree;
import org.mitre.bio.phylo.tree.NodeUtils;
import org.mitre.bio.phylo.tree.Tree;
import org.mitre.bio.phylo.tree.UPGMATree;

import org.mitre.bio.phylo.tree.io.NewickWriter;
import org.mitre.clustering.AffinityPropagation;

import weka.core.matrix.Matrix;

/**
 * Main Command Line Inteface Class for ccvs
 * 
 * @author Marc Colosimo
 * @author Matt Peterson
 */
public class CompleteCompositionVectorMain {

    /** Set-up our logger. */
    private static final Log LOG = LogFactory.getLog(CompleteCompositionVectorMain.class);
    public FastaIterator seqIter = null;
    public Integer begin = 3;
    public Integer end = 9;
    public Integer topNMers = null;
    public Integer seqNameParser = 1;         // what to use for sequence name
    /** What distance calculation to use */
    public Integer distCalc = 2;            // default: Cosine
    public Integer prefVal = 1;             // default: Median
    public Boolean upgma = false;           // default: Neighbor-Joined Tree
    public String nwkOutFile = null;        // default: no output as newick
    public String xmlOutFile = null;        // default: no output as PhyloXML
    public String apClusterOutfile = null;  // default: Don't do ap clustering
    public String matrixOutFile = null;     // default: Don't write the matrix out
    public String nmersOutFile = null;      // default: Don't write nmers
    public String vectorsOutFile = null;     // default: Don't write vectors to a json file
    public String vectorsInFile = null;     // default: no input from a json file
    public String entOutFile = null;        // default: Don't write entropy
    public Boolean embeddedSQL = false;     // default: Use memory, not derby
    public Boolean useSQL = false;          // default: if both SQLs set, this wins
    private Level logginLevel = Level.WARN; // default: Log level WARN


    /**
     * Set-up our static strings
     */
    private static String APPLICATION_TITLE = "CompleteCompositionVectorCLI";
    private static String APPLICATION_VERSION = "Unknown";
    private static String APPLICATION_BUILD = "Unknown";

    static {
        ResourceBundle bundle = null;
        try {
            bundle = ResourceBundle.getBundle("org.mitre.ccv.ApplicationInformation");
        } catch (MissingResourceException mre) {
            bundle = null;
        }
        if (bundle != null) {
            APPLICATION_VERSION = bundle.getString("application.version");
            APPLICATION_TITLE = APPLICATION_TITLE + " " + APPLICATION_VERSION;

            APPLICATION_BUILD = bundle.getString("subversion.version");
        }
    }

    /**
     * Returns a parsed sequence name usually from a FASTA file
     * 
     * @param seq the {@link Sequence}
     * @param seqParser 1-name(default);2-description;3-full header(name + description, which might break newick-style trees)
     */
    static public String parseSequenceName(Sequence seq, int seqParser) {
        String name = null;
        switch (seqParser) {
            case 3:
                name = seq.getName() + " " + seq.getDescription();
                break;
            case 2:
                name = seq.getDescription();
                break;
            case 1:
            default:
                name = seq.getName();
        }

        /** We need to return something */
        if (name == null) {
            if (seq.getName() == null) {
                return "[No Name]";
            } else {
                return "[Null]" + seq.getName();
            }
        } else {
            return name;
        }
    }

    /**
     * Generate the ccv set with the set parameters.
     * 
     * The parameters can be set on the command-line or read in from
     * the "ccv.properties" file.
     */
    public VectorSet generateCompleteCompositionVectorSet() throws Exception {

        VectorSet set = null;
        if (this.useSQL) {

            Properties prop;
            try {
                prop = getSQLProperties();
                set = new VectorSetSQL(this.begin, this.end, prop);
            } catch (IOException ioe) {
                throw new Exception("Unable to load ccv.properties file!");
            } catch (SQLException slqe) {
                AbstractVectorSetSQL.printSQLException(slqe);
                throw new Exception("Unable to connect to database!");
            }

        } else if (this.embeddedSQL) {
            set = new EmbeddedVectorSetSQL(this.begin, this.end);
        } else {
            set = new IndexedCompleteCompositionVectorSet(this.begin, this.end);
        }

        return set;
    }


    /**
     * Returns a {@link DistanceMatrix} using the set distance method
     * 
     * @param matrix <code>CompleteMatrix</code> of sequence vectors
     * @param nmers the top number of nmers to use 
     * (<code>null</code> value uses all).
     */
    public DistanceMatrix createDistanceMatrix(
            VectorSet set, CompleteMatrix matrix) {

        try {
            switch (this.distCalc) {
                case 4:
                    if (this.topNMers == null) {
                        // much faster if we don't try to limit it to a set of nmers
                        return set.createJaccardDistanceMatrix(null);
                    } else {
                        return set.createJaccardDistanceMatrix(matrix.getNmers());
                    }
                case 3:
                    return set.createESDistanceMatrix(matrix);
                case 2:
                    return set.createCosineDistanceMatrix(matrix);
                case 1:
                default:
                    return set.createEuclidianDistanceMatrix(matrix);
            }
        } catch (OutOfMemoryError e) {
            String errStr = "Out of memory while creating distance matrix!\n" +
                    "Try limiting top nmers or increasing heap space or " +
                    "changing the number of windows (begin/end).";
            LOG.fatal(errStr, e);
            return null;
        }
    }

    /**
     * Check to see if we are outputing a tree, if not then we do not calculate it
     * 
     * @return
     */
    public Boolean calculateTree() {
        /** Things to check: xmlOutFile, nwkOutFile */
        if (this.xmlOutFile != null || this.nwkOutFile != null) {
            return true;
        }
        return false;
    }

    /**
     * Check to see if we need to calculate distances. 
     * 
     * If we are outputing a tree, matrixOutFile, apClusterOutfile, then true.
     * @return
     */
    public Boolean calculateDistances() {
        if (calculateTree()) {
            return true;
        }
        if (this.matrixOutFile != null || this.apClusterOutfile != null) {
            return true;
        }

        return false;
    }

    /**
     * Check to see if need to calculate/generate the full vector matrix
     * @return true if we do
     */
    public Boolean calculateMatrix() {
        if (this.calculateDistances()) {
            return true;
        }
        if (this.vectorsOutFile != null || this.nmersOutFile != null) {
            return true;
        }
        return false;
    }

    /**
     * Create a tree using the given distance matrix
     * 
     * @param distMatrix
     * @return the tree (NJ is default, UPGMA optional)
     */
    public Tree createTree(DistanceMatrix distMatrix) {
        if (distMatrix == null) {
            return null;
        }

        Tree tree = null;
        if (this.upgma) {
            tree = new UPGMATree(distMatrix);
            LOG.warn("UPGMA is deprecated, should use ClusterTree!");
        } else {
            /** Build a NeighborJoined Tree */
            Tree njTree = new NeighborJoiningTree(distMatrix);
            tree = njTree.getMidpointRootedTree();
        }
        return tree;
    }

    /**
     * Calculates the median value of a 1-D array
     * 
     * @param array
     * @return
     */
    public double getPreference(double[] array, int type) {
        java.util.Arrays.sort(array);

        int arrayLength = array.length;

        if (type == 0) {
            return array[0];
        } else {

            double median;

            if (arrayLength % 2 == 0) {
                int n = (arrayLength + 1) / 2;
                median = (array[n] + array[n + 1]) / 2.0;
            } else {
                int n = arrayLength / 2;
                median = array[n];
            }

            if (type == 1) {
                return median;
            } else {
                return (median + array[0]) / 2;
            }

        }
    }

    /**
     * Writes a PAL tree in PhyloXML format to given BufferedWriter
     * 
     * @param tree the Tree
     * @param bw the BufferedWriter
     */
    public static void writePhyloXMLTree(Tree tree, BufferedWriter bw) throws IOException {

        Forest f = NodeUtils.getSingleton().convert2Forest(tree.getRoot());

        /**
         * Needs to be static because others call this: should be a util
         * Convert tree string to forest
         */
        OutputFormat of = new OutputFormat("XML", "ISO-8859-1", true);
        of.setIndent(1);
        of.setIndenting(true);

        XMLSerializer serial = new XMLSerializer(bw, of);
        if (f != null) {
            try {
                serial.serialize(f.getOwnerDocument());
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        bw.flush();
    }

    /**
     * Write the tree to a newick style file
     * 
     * @param tree the Tree.
     * @param bw BufferedWriter to write tree to.
     */
    public static void writeNwkTree(Tree tree, BufferedWriter bw) {
        try {
            NewickWriter.getSingleton().print(new PrintWriter(bw), tree.getRoot(), true, false);
            bw.flush();
        } catch (Exception ioe) {
            LOG.error("Error in writing out tree to newick file.", ioe);
        }
    }

    /**
     * Performs Affinity Propagation Clustering
     * 
     * @param dm
     * @param filename
     */
    public AffinityPropagation cluster(DistanceMatrix dm, int type) {
        double[][] mVals = dm.getClonedDistances();
        Matrix m = new Matrix(mVals);
        if (distCalc == 1 || distCalc == 3) {
            m = m.times(-1.0);
        } else if (distCalc == 2) {
            Matrix o = new Matrix(m.getRowDimension(), m.getColumnDimension(), 1.0);
            m = o.minus(m);
        }

        int total = m.getColumnDimension();
        double values[] = new double[(total * total - total) / 2];
        int count = 0;
        for (int i = 1; i < total; i++) {
            for (int j = i + 1; j < total - 1; j++) {
                values[count] = m.get(i, j);
                count++;
            }

        }

        double preference = getPreference(values, type);

        AffinityPropagation ap = new AffinityPropagation(m,
                5000, 300, 0.9, preference);

        return ap;
    }

    /**
     * Write out the distance matrix to the given BufferedWriter.
     * 
     * @param dm the DistanceMatrix.
     * @param bw the BufferedWriter to write distance to.
     *
     */
    public void writeDistanceMatrix(DistanceMatrix dm, BufferedWriter bw) throws IOException {

        double[][] mVals = dm.getClonedDistances();
        Matrix m = new Matrix(mVals);
        /** was when this was writeSimilarityMatrix
        if (distCalc == 1 || distCalc == 3) {
            m = m.times(-1.0);
        } else if (distCalc == 2 || distCalc == 4) {
            Matrix o = new Matrix(m.getRowDimension(), m.getColumnDimension(), 1.0);
            m = o.minus(m);
        }
        */
        try {
            m.write(bw);
            // Write the matrix to the file.
            /**
            bw.write(this.distCalc.toString() + "\n");
            for (int i = 0; i < m.getRowDimension(); i++) {
                for (int j = 0; j < m.getColumnDimension(); j++) {

                    String s = String.format("%03d\t%03d\t%f\n", i + 1, j + 1, m.get(i, j));

                    bw.write(s);
                }
            }
             */
            bw.close();

        } catch (Exception ex) {
           throw new IOException(ex.getMessage());
        }
    }

    /**
     * Gets the properties for SQL access.
     * <P>
     * This <B>DOES NOT</B> check any of the fields.
     * <P>
     * @see org.mitre.bio.ccv.sql.VectorSet for field descriptions.
     * @return java.util.Properties;
     */
    public Properties getSQLProperties() throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream("ccv.properties"));
        return props;
    }

    /**
     * Loads the 'ccv.properties' file and initalizes from the fields.
     * <P>
     * This <B>DOES NOT</B> check any of the jdbc fields, only ccv fields.
     * <P>
     * @return
     */
    public Properties loadProperties() throws IOException {
        Properties props = new Properties();

        props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("ccv.properties"));

        /** Load */
        return props;
    }

    /**
     * Not the best way, but one way to set logging without changing the properties file
     * 
     * @param level
     */
    public void setLoggingLevel(Level level) {
        this.logginLevel = level;
        // using log4j's Logger!
        Logger rootLogger = Logger.getRootLogger();
        LOG.info("Setting root logger to " + level.toString());
        rootLogger.setLevel(level);
    }

    @SuppressWarnings("static-access")
    public static void main(String[] argv) throws Exception {

        CompleteCompositionVectorMain ccvm =
                new CompleteCompositionVectorMain();

        /** What we are */
        String cli_title = APPLICATION_TITLE + " [build: " + APPLICATION_BUILD +
                "]\n(C) Copyright 2007-2010, by The MITRE Corporation.";

        /** create the Options */
        Options options = new Options();
        options.addOption(
                OptionBuilder.withArgName("file").hasArg(true).withDescription("use given file for generating ccv").create("file"));
        options.addOption(
                OptionBuilder.withArgName("number").hasArg(true).withDescription("number of top nmers to use in calculations").create("topNmers"));
        options.addOption(
                OptionBuilder.withArgName("begin").hasArg(true).withDescription("initial length of tile").create("begin"));
        options.addOption(
                OptionBuilder.withArgName("end").hasArg(true).withDescription("ending length of title").create("end"));
        options.addOption(
                OptionBuilder.withArgName("number").hasArg(true).withDescription("what name to use: 1-name(default);2-description;3-full header(might break newick-style trees)").create("name"));
        options.addOption(
                OptionBuilder.withArgName("number").hasArg(true).withDescription("what distance calculation " +
                "to use: 1-euclidian;2-cosine(default);" +
                "3-ESDistance;4-Jaccard").create("distance"));
        options.addOption(
                OptionBuilder.withArgName("upgma").hasArg(false).withDescription("Generate UPGMA tree (default is neighor-joined)").create("upgma"));

        options.addOption(
                OptionBuilder.withArgName("file").hasArg(true).withDescription("Do affinity propagation and write cluster values to given file name").create("cluster"));
        options.addOption(
                OptionBuilder.withArgName("file").hasArg(true).withDescription("File to write entropies to").create("entfile"));

        options.addOption(
                OptionBuilder.withArgName("file").hasArg(true).withDescription("File to write distance matrix to").create("distfile"));

        options.addOption(
                OptionBuilder.hasArg(true).withArgName("file").withDescription("Write tree in PhyloXML format to file").create("xml"));

        options.addOption(
                OptionBuilder.hasArg(true).withArgName("file").withDescription("Write tree in nwk format to file").create("nwk"));

        options.addOption(
                OptionBuilder.hasArg(false).withDescription("Use an embedded (Derby) SQL server").create("embeddedSQL"));

        options.addOption(
                OptionBuilder.hasArg(false).withDescription("Use SQL server. Properties are defined in ccv.properties.").create("sql"));

        options.addOption(
                OptionBuilder.hasArg(true).withArgName("file").withDescription("JSON file to write out nmers to").create("nmersfile"));

        options.addOption(
                OptionBuilder.hasArg(true).withArgName("file").withDescription("JSON file to write out vectors to " +
                "(Overrides nmersout, only one file will be written).").create("vectorsout"));

        options.addOption(
                OptionBuilder.hasArg(true).withArgName("file").withDescription("JSON file to read in vectors from").create("vectorsin"));
        
        options.addOption(
                OptionBuilder.withArgName("help").hasArg(false).withDescription("Print this message").create("help"));

        options.addOption(
                OptionBuilder.withArgName("number").hasArg(true).withDescription("What preference to use: 0-min 1-median 2-avg(min,med): default is median").create("prefval"));
        options.addOption(
                OptionBuilder.withArgName("level").hasArg(true).withDescription("Set the logging (verbosity) level: OFF, FATAL, ERROR, WARN, INFO, DEBUG. TRACE, ALL (none to everything)").create("verbosity"));

        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();

        // create the parser
        CommandLineParser parser = new GnuParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, argv);
            if (line.getOptions().length == 0 ||
                    line.hasOption("help")) {
                System.out.println(cli_title);
                formatter.printHelp("ccv [options] -file ", options);
                return;
            }
            if (line.hasOption("verbosity")) {
                // if this fails then they get DEBUG!
                ccvm.setLoggingLevel(Level.toLevel(line.getOptionValue("verbosity")));
            }
            if (line.hasOption("file")) {
                String fileName = line.getOptionValue("file");
                BufferedReader br = new BufferedReader(new FileReader(fileName));
                ccvm.seqIter = new FastaIterator(br);
            }
            if (line.hasOption("vectorsin")) {
                ccvm.vectorsInFile = line.getOptionValue("vectorsin");
            }
            if (line.hasOption("topNmers")) {
                try {
                    ccvm.topNMers =
                            Integer.parseInt(line.getOptionValue("topNmers"));
                } catch (NumberFormatException nfe) {
                    System.err.println(
                            "Error parsing topNmers option. Reason: " +
                            nfe.getMessage());
                    return;
                }
            }
            if (line.hasOption("begin")) {
                try {
                    ccvm.begin =
                            Integer.parseInt(line.getOptionValue("begin"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException(
                            "Error parsing 'begin' option. Reason: " +
                            nfe.getMessage());
                }
            }
            if (line.hasOption("end")) {
                try {
                    ccvm.end =
                            Integer.parseInt(line.getOptionValue("end"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException(
                            "Error parsing 'end' option. Reason: " +
                            nfe.getMessage());
                }
            }
            if (line.hasOption("name")) {
                try {
                    ccvm.seqNameParser =
                            Integer.parseInt(line.getOptionValue("name"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException(
                            "Error parsing 'name' option. Reason: " +
                            nfe.getMessage());
                }
            }
            if (line.hasOption("distance")) {
                try {
                    ccvm.distCalc =
                            Integer.parseInt(line.getOptionValue("distance"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException(
                            "Error parsing 'distance' option. Reason: " +
                            nfe.getMessage());
                }
            }
            if (line.hasOption("upgma")) {
                ccvm.upgma = true;
            }
            if (line.hasOption("cluster")) {
                ccvm.apClusterOutfile = line.getOptionValue("cluster");
            }
            if (line.hasOption("distfile")) {
                ccvm.matrixOutFile = line.getOptionValue("distfile");
            }
            if (line.hasOption("xml")) {
                ccvm.xmlOutFile = line.getOptionValue("xml");
            }
            if (line.hasOption("nwk")) {
                ccvm.nwkOutFile = line.getOptionValue("nwk");
            }
            if (line.hasOption("embeddedSQL")) {
                ccvm.embeddedSQL = true;
            }
            if (line.hasOption("sql")) {
                ccvm.useSQL = true;
            }
            if (line.hasOption("entfile")) {
                ccvm.entOutFile = line.getOptionValue("entfile");
            }

            /** 
             * This is done inside creatDistanceMatrix.
             * The order of nmers and vectors is important!
             */
            if (line.hasOption("nmersfile")) {
                ccvm.nmersOutFile = line.getOptionValue("nmersfile");
            }
            if (line.hasOption("vectorsout")) {
                ccvm.vectorsOutFile = line.getOptionValue("vectorsout");
                ccvm.nmersOutFile = null;
            }

            if (line.hasOption("prefval")) {
                try {
                    ccvm.prefVal = Integer.parseInt(line.getOptionValue("prefval"));
                } catch (NumberFormatException nfe) {
                    throw new ParseException(
                            "Error parsing 'prefval' option. Reason: " +
                            nfe.getMessage());
                }
            }

        } catch (ParseException exp) {
            // oops, something went wrong
            System.out.println(cli_title);
            System.out.println("Invalid option!  Reason: " + exp.getMessage());
            formatter.printHelp("ccv [options] -file ", options);
            return;
        }

        /** Print out who we are */
        LOG.info(cli_title);

        VectorSet set = null;
        DistanceMatrix distMatrix = null;
        CompleteMatrix matrix = null;

        if (ccvm.vectorsInFile != null) {
            LOG.info("Reading in CompleteCompositionVectors from " + ccvm.vectorsInFile);
            // we only save the data not everything that is in the vectorSet
            BufferedReader br = new BufferedReader(new FileReader(ccvm.vectorsInFile));
            matrix = CompleteMatrix.readJsonCompleteMatrix(br);
            br.close();
            ccvm.begin = matrix.getBegin();
            ccvm.end = matrix.getEnd();
            LOG.info(String.format("Loaded in %d samples and %d nmers (features)",
                    matrix.getNames().size(), matrix.getNmers().size()));

            /** just make an empty set */
            set = ccvm.generateCompleteCompositionVectorSet();

            /**
             * Need to think about this - should be able to use the nmer list
             * directly in generating the complete matrix then merge the two
             */
            if (ccvm.seqIter != null) {
                LOG.warn("Unable to process new samples when given a JSON vector set!");
            }
        } else {
            LOG.info("Generating complete composition vector set...");
            set = ccvm.generateCompleteCompositionVectorSet();

            /** If we have a sequnece iterator (i.e. a fasta file) then process those samples */
            if (ccvm.seqIter != null) {
                while (ccvm.seqIter.hasNext()) {
                    Sequence s = null;
                    try {
                        s = ccvm.seqIter.next();
                    } catch (NoSuchElementException e) {
                        LOG.fatal("Iteration error in sequence file!", e);
                        return;
                    }
                    String seqString = s.seqString();
                    set.addSequence(parseSequenceName(s, ccvm.seqNameParser), seqString);
                }
            }
        }

        if (!ccvm.calculateMatrix()) {
            LOG.info("No other operations left so finished!");
            return;
        }

        if (matrix == null) {
            LOG.info("Generating complete matrix of nmers...");
            try {
                matrix = set.getFullMatrix(ccvm.topNMers, ccvm.entOutFile);
            } catch (OutOfMemoryError e) {
                LOG.fatal(
                        "\nOut of memory while getting full matrix!\n" +
                        "Try limiting top nmers, increasing heap space, or " +
                        "changing the number of windows (begin/end).", e);
                return;
            }
        } // we loaded it in
        /**
         * set.getFullMatrix(matrix.getNmers)
         * and merge the two
         */

        BufferedWriter bw;  // Used in several places

        /** Write out vectors or nmers before going on */
        if (ccvm.vectorsOutFile != null && ccvm.vectorsOutFile.length() != 0) {
            LOG.info(String.format("Writing %d nmers and %d vectors to file %s\n",
                    matrix.getNmers().size(), matrix.getNames().size(),
                    ccvm.vectorsOutFile));
            //matrix.writeJSONObject(matrix.writeJsonCompleteMatrix(), ccvm.vectorsOutFile);
            bw = new BufferedWriter(new FileWriter(ccvm.vectorsOutFile));
            matrix.writeJsonCompleteMatrix(bw);
            bw.close();
        } else if (ccvm.nmersOutFile != null && ccvm.nmersOutFile.length() != 0) {
            LOG.info(String.format("Writing %d nmers to file %s\n",
                    matrix.getNmers().size(), ccvm.nmersOutFile));
            //matrix.writeJSONObject(matrix.writeJsonNmers(), ccvm.nmersOutFile);
            bw = new BufferedWriter(new FileWriter(ccvm.nmersOutFile));
            matrix.writeJsonNmers(bw);
            bw.close();
        }

        /** Build distance matrix */
        if (!ccvm.calculateDistances()) {
            LOG.info("Done generating vector matrix. No other operations left so finished!");
            return;
        }
        LOG.info("Generating distance matrix...");
        distMatrix = ccvm.createDistanceMatrix(set, matrix);

        if (ccvm.calculateTree() && set.getSampleNames().size() > 2 ) {
            LOG.info("Creating tree");

            Tree tree = ccvm.createTree(distMatrix);
            if (ccvm.nwkOutFile != null) {
                try {
                    bw = new BufferedWriter(new FileWriter(ccvm.nwkOutFile));
                    ccvm.writeNwkTree(tree, bw);
                    bw.close();
                } catch (Exception ioe) {
                    LOG.error(String.format(
                            "Error in writing tree to newick file '%s'!",
                            ccvm.nwkOutFile), ioe);
                }
            }

            /** We can output both */
            if (ccvm.xmlOutFile != null) {
                try {
                    bw = new BufferedWriter(new FileWriter(ccvm.xmlOutFile));
                    ccvm.writePhyloXMLTree(tree, bw);
                    bw.close();
                } catch (IOException ioe) {
                    LOG.error(String.format(
                            "Error in writing tree to phyloXML file '%s'!",
                            ccvm.xmlOutFile), ioe);
                }
            }
        } else if ( ccvm.calculateTree() && set.getSampleNames().size() <= 2) {
                LOG.error("Cannot build a tree with less than 3 samples!");
        }

        if (ccvm.matrixOutFile != null) {
            LOG.info("Writing distance matrix to file.");
            try {
                ccvm.writeDistanceMatrix(distMatrix,
                        new BufferedWriter(new FileWriter(ccvm.matrixOutFile)));
            } catch (IOException ioe) {
                LOG.error(String.format(
                        "Error in writing distances to file '%s'!",
                        ccvm.matrixOutFile), ioe);
            }
        }

        /** 
         * Run AP Clustering 
         * 
         * Cluster file format: sample_name(String)<tab>cluster_id(Integer)
         */
        if (ccvm.apClusterOutfile != null) {
            LOG.info("Running affinity propagation clustering...");
            AffinityPropagation ap = ccvm.cluster(distMatrix, ccvm.prefVal);
            int[] clusters = ap.getClusters();
            try {
                bw = new BufferedWriter(new FileWriter(ccvm.apClusterOutfile));
                for (int c = 0; c < clusters.length; c++) {
                    //String id = distMatrix.getIdentifier(c).getName();
                    String id = distMatrix.getIdentifier(c);
                    bw.write(id + "\t" + Integer.toString(clusters[c]) + "\n");

                }
                bw.close();
            } catch (IOException ioe) {
                LOG.error(String.format(
                        "Error in writing clustering results to the file '%s'!",
                        ccvm.apClusterOutfile), ioe);
            }
        }
        LOG.info("Finished!");
    }
}

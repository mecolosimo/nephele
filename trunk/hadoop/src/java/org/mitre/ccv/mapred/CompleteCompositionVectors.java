/**
 * Created on April 8, 2009.
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
package org.mitre.ccv.mapred;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

import org.mitre.bio.mapred.TotalSequenceLength;
import org.mitre.mapred.fs.FileUtils;

/**
 * Class that generates complete compostion vectors and feature vectors.
 *
 * <p>Some properties (<code>-D property=value</code>) supported are:
 * <ul>
 * <li>None</li>
 * </ul>
 *
 * @author Marc Colosimo
 */
public class CompleteCompositionVectors extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(CompleteCompositionVectors.class);
    /**
     * File name for storing k-mer counts.
     */
    public static final String KMER_COUNTS = "kmer_cnts";
    /**
     * File name for storing total  sequence length (all of the samples).
     */
    public static final String TOTAL_LENGTH = "length";
    /**
     * File name for storing k-mer probabilities
     */
    public static final String KMER_PROBABILITIES = "kmer_probs";
    /**
     * File name for storing inverted k-mer probabilities
     */
    public static final String INVERTED_KMER_PROBABILITIES = "invt_kmer_probs";
    /**
     * File name for storing k-mer pi values
     */
    public static final String KMER_PI_VALUES = "kmer_pi_values";
    /**
     * File name for storing sequence composition vectors
     */
    public static final String COMPOSITION_VECTORS = "composition_vectors";
    /**
     * File name for storing k-mer entropy values
     */
    public static final String ENTROPY_VALUES = "kmer_entropy_values";
    /**
     * File name for storing sorted (reverse natural order) k-mer entropy values
     */
    public static final String SORTED_ENTROPY_VALUES = "sorted_kmer_entropy_values";
    /**
     * File name for storing reduce set of k-mers in sorted order (usually prefixed with the number)
     */
    public static final String KMER_ENTROPY_SET = "_kmer_entroy_set";
    /**
     * File name for storing generated sparse k-mer features vectors
     * (usually a reduced k-mer set from the composition vectors).
     */
    public static final String FEATURE_VECTORS = "feature_vectors";
    /**
     * File name for writting out feature vectors to a JSON file.
     */
    public static final String JSON = "json";

    /**
     *
     * The JSO data will be the same as {@link org.mitre.ccv.CompleteMatrix#jsonCompleteMatrix}, but the features
     * will be in a different order. This version, by default sorts, only by entropy values, whereas the
     * ccv in-memory version sorts by the k-mer natural order (i.e., lexigraphic).
     * @param argv
     * @return
     * @throws java.lang.Exception
     */
    @Override
    @SuppressWarnings("static-access")    // For OptionBuilder
    public int run(String[] argv) throws Exception {
        JobConf conf = new JobConf(getConf());
        String cli_title = "CompleteCompositionVectorHadoop";

        int start = CalculateKmerCounts.DEFAULT_START;
        int end = CalculateKmerCounts.DEFAULT_END;
        int topkmers = 0;

        String input = null;
        String output = null;
        String vectorJsonOutput = null;
        //String kmerJsonOutput = null;

        boolean cleanLogs = false;

        /** create the Options */
        Options options = new Options();

        /** Hadoop Options */
        options.addOption(
                OptionBuilder.withArgName("number").hasArg(true).withDescription("number of maps").create("m"));
        options.addOption(
                OptionBuilder.withArgName("number").hasArg(true).withDescription("number of reducers").create("r"));
        
        // org.hadoop.util.GenericOptionsParser should captures this, but it doesn't
        options.addOption(OptionBuilder.withArgName("property=value").hasArg(true).withValueSeparator().withDescription("use value for given property").create("D"));

        /** CompleteCompositionVector Options */
        options.addOption(
                OptionBuilder.withArgName("number").hasArg(true).withDescription("number of top k-mers to use in calculations").create("topKmers"));
        options.addOption(
                OptionBuilder.withArgName("start").hasArg(true).withDescription("starting length of tile").create("start"));
        options.addOption(
                OptionBuilder.withArgName("end").hasArg(true).withDescription("ending length of title").create("end"));
        options.addOption(
                OptionBuilder.hasArg(true).withArgName("file").withDescription("JSON file to write out k-mers to").create("kmersfile"));

        options.addOption(
                OptionBuilder.hasArg(true).withArgName("file").withDescription("JSON file to write out feature vectors to " +
                "(Overrides kmersout, only one file will be written).").create("vectorsfile"));

        options.addOption(
                OptionBuilder.withArgName("number").hasArg(true).withDescription("What preference to use: 0-min 1-median 2-avg(min,med): default is median").create("prefval"));

        options.addOption(
                OptionBuilder.withArgName("help").hasArg(false).withDescription("print this message").create("help"));



        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();

        //GenericOptionsParser gop = new GenericOptionsParser(conf, options, argv);
        GenericOptionsParser gop = new GenericOptionsParser(conf, argv);

        String[] remaining_args = gop.getRemainingArgs();

        // create the parser
        CommandLineParser parser = new GnuParser();
        //CommandLine line = gop.getCommandLine();
        String[] other_args = new String[]{};

        try {
            CommandLine line = parser.parse(options, remaining_args);
            other_args = line.getArgs();

            // Make sure there is a parameter left.
            if (other_args.length == 0) {
                System.out.println(cli_title);
                System.out.println("Missing input path!");
                formatter.printHelp("hccv [options] <input> [<output>] ", options);
                GenericOptionsParser.printGenericCommandUsage(System.out);
                return -1;
            }

            Option[] opts = line.getOptions();
            if (line.hasOption("help")) {
                System.out.println(cli_title);
                formatter.printHelp("hccv [options] <input> [<output>] ", options);
                GenericOptionsParser.printGenericCommandUsage(System.out);
                return -1;
            }

            // could also use line.iterator()
            for (Option opt : opts) {
                if (opt.getOpt().equals("m")) {
                    conf.setNumMapTasks(Integer.parseInt(opt.getValue()));
                }
                if (opt.getOpt().equals("r")) {
                    conf.setNumReduceTasks(Integer.parseInt(opt.getValue()));
                }
                if (opt.getOpt().equals("D")) {
                    // We can have multiple properties we want to set
                    String[] properties = opt.getValues();
                    for (String property : properties) {
                        String[] keyval = property.split("=");
                        conf.set(keyval[0], keyval[1]);
                    }
                }
                if (opt.getOpt().equals("start")) {
                    start = Integer.parseInt(opt.getValue());
                }
                if (opt.getOpt().equals("end")) {
                    end = Integer.parseInt(opt.getValue());
                }
                if (opt.getOpt().equals("topKmers")) {
                    topkmers = Integer.parseInt(opt.getValue());
                }
                if (opt.getOpt().equals("vectorsfile")) {
                    vectorJsonOutput = opt.getValue();
                }
            }
        } catch (ParseException e) {
            LOG.warn("options parsing faild: " + e.getMessage());
            System.out.println(cli_title);
                formatter.printHelp("hccv [options] <input> [<output>] ", options);
                GenericOptionsParser.printGenericCommandUsage(System.out);
        }
        if (start <= 2) {
            throw new IllegalArgumentException("Value of 'start' argument must be larger than 2");
        }

        input = other_args[0];
        if (other_args.length < 2) {
            output = input + "_" + FileUtils.getSimpleDate();
        } else {
            output = other_args[2];
        }

        /**
         * Check output path. Either needs to exist as a directory or not exist
         */
        Path outputPath = new Path(output);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        } else if (fs.exists(outputPath) || !fs.getFileStatus(outputPath).isDir()) {
            LOG.fatal(String.format("Output directory %s already exists", outputPath.makeQualified(fs)));
            throw new FileAlreadyExistsException(
                    String.format("Output directory %s already exists", outputPath.makeQualified(fs)));
        }

        String outputDir = output + Path.SEPARATOR;

        int res;
        /**
         * Zero, CalculateCompositionVectors
         */
        LOG.info("Starting CalculateCompositionVectors Map-Reduce job");
        CalculateCompositionVectors cv = new CalculateCompositionVectors();
        res = cv.initJob(conf, start, end, input, outputDir + COMPOSITION_VECTORS, cleanLogs);
        if (res != 0) {
            LOG.info("CalculateCompositionVectors returned non-zero result!");
            return res;
        }
        // We can stop now or continue to reduce dimensionallity using RRE or other means

        /**
         * First, CalculateKmerCounts
         */
        LOG.info("Starting CalculateKmerCounts Map-Reduce job");
        // FastMap option for CalculateKmers!?!
        CalculateKmerCounts ckc = new CalculateKmerCounts();
        res = ckc.initJob(conf, start, end, input, outputDir + KMER_COUNTS);
        if (res != 0) {
            LOG.fatal("CalculateKmerCounts returned non-zero result!");
            return res;
        }

        /**
         * Second, TotalSequenceLength
         */
        LOG.info("Starting TotalSequenceLength Map-Reduce job");
        TotalSequenceLength tsl = new TotalSequenceLength();
        res = tsl.initJob(conf, input, outputDir + TOTAL_LENGTH, cleanLogs);
        if (res != 0) {
            LOG.fatal("TotalSequenceLength returned non-zero result!");
            return res;
        }
        int length = tsl.getCount(conf, outputDir + TOTAL_LENGTH);

        if (length < 3) {
            LOG.fatal("TotalSequenceLength returned a total sequence length of less than 3.");
            return -1;
        } else {
            LOG.info(String.format("TotalSequenceLength returned a total sequence length of %d.", length));
        }

        /**
         * Third, CalculateKmerProbabilities
         */
        LOG.info("Starting CalculateKmerProbabilities Map-Reduce job");
        CalculateKmerProbabilities ckp = new CalculateKmerProbabilities();
        res = ckp.initJob(conf, start, end, length, outputDir + KMER_COUNTS, outputDir + KMER_PROBABILITIES, cleanLogs);
        if (res != 0) {
            LOG.fatal("CalculateKmerProbabilities returned non-zero result!");
            return res;
        }

        /**
         * Fourth, InvertKmerProbabilities
         */
        LOG.info("Starting InvertKmerProbabilities Map-Reduce job");
        InvertKmerProbabilities ikp = new InvertKmerProbabilities();
        res = ikp.initJob(conf, outputDir + KMER_PROBABILITIES, outputDir + INVERTED_KMER_PROBABILITIES, cleanLogs);
        if (res != 0) {
            LOG.fatal("InvertKmerProbabilities returned non-zero result!");
            return res;
        }

        /**
         * Fifth, CalculateKmerPiValues
         */
        LOG.info("Starting CalculateKmerPiValues Map-Reduce job");
        CalculateKmerPiValues kpv = new CalculateKmerPiValues();
        res = kpv.initJob(conf, start, end, outputDir + INVERTED_KMER_PROBABILITIES, outputDir + KMER_PI_VALUES, cleanLogs);
        if (res != 0) {
            LOG.fatal("CalculateKmerPiValues returned non-zero result!");
            return res;
        }

        /**
         * Sixth,CalculateKmerRevisedRelativeEntropy
         */
        LOG.info("Starting CalculateKmerRevisedRelativeEntropy Map-Reduce job");
        CalculateKmerRevisedRelativeEntropy krre = new CalculateKmerRevisedRelativeEntropy();
        res = krre.initJob(conf, outputDir + KMER_PI_VALUES, outputDir + COMPOSITION_VECTORS, outputDir + ENTROPY_VALUES, cleanLogs);
        if (res != 0) {
            LOG.fatal("CalculateKmerRevisedRelativeEntropy returned non-zero result!");
            return res;
        }

        /**
         * Seventh, SortKmerRevisedRelativeEntropies
         */
        SortKmerRevisedRelativeEntropies srre = new SortKmerRevisedRelativeEntropies();
        res = srre.initJob(conf, outputDir + ENTROPY_VALUES, outputDir + SORTED_ENTROPY_VALUES, cleanLogs);
        if (res != 0) {
            LOG.fatal("SortKmerRevisedRelativeEntropies returned non-zero result!");
            return res;
        }

        /**
         * Eigth, GenerateFeatureVectors
         *
         * Generate a flatten list to add to the cache to be distributed to the map-tasks.
         */
        Path listOutputPath = new Path(outputDir + Integer.toString(topkmers) + KMER_ENTROPY_SET);
        LOG.info(String.format("Loading %d sorted k-mers from %s to %s", topkmers, outputDir + SORTED_ENTROPY_VALUES, listOutputPath.toString()));
        int num = CompleteCompositionVectorUtils.flattenKmerEntropySequenceFile(conf, topkmers, outputDir + SORTED_ENTROPY_VALUES, listOutputPath.toString(), cleanLogs);

        if (num != topkmers) {
            LOG.fatal(String.format("Requested %d k-mers, but got %d. Using %d", topkmers, num, num));
            topkmers = num;
        }
        GenerateFeatureVectors fv = new GenerateFeatureVectors();
        res = fv.initJob(conf, listOutputPath.toString(), topkmers, outputDir + COMPOSITION_VECTORS, outputDir + FEATURE_VECTORS, cleanLogs);
        if (res != 0) {
            LOG.fatal("GenerateFeatureVectors returned non-zero result!");
            return res;
        }

        /**
         * Save feature vectors, features (k-mers), and properties to a JSON file.
         *
         * The data will be the same as {@link org.mitre.ccv.CompleteMatrix#jsonCompleteMatrix}, but the features
         * will be in a different order. This version, by default sorts, only by entropy values, whereas the
         * ccv in-memory version sorts by the k-mer natural order (i.e., lexigraphic).
         */
        if (vectorJsonOutput != null && vectorJsonOutput.length() > 0) {
            LOG.info("Writing features out to " + vectorJsonOutput);
            CompleteCompositionVectorUtils.featureVectors2Json(conf, start, end,
                    topkmers, outputDir + SORTED_ENTROPY_VALUES,
                    outputDir + FEATURE_VECTORS, vectorJsonOutput);
        }

        LOG.info("All done generating complete composition vectors and feature vectors.");
        return res;
    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CompleteCompositionVectors(), args);
        System.exit(res);
    }
}

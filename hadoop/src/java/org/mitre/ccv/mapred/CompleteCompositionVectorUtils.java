/**
 * Created on April 9, 2009.
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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

import org.mitre.ccv.mapred.io.KmerEntropyPairWritable;

import org.mitre.la.SparseVector;
import org.mitre.la.mapred.io.SparseVectorWritable;
import org.mitre.mapred.fs.FileUtils;

/**
 * A class of Complete Composition Vector utility methods.
 *
 * @author Marc Colosimo
 */
public class CompleteCompositionVectorUtils extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(CompleteCompositionVectorUtils.class);

    private CompleteCompositionVectorUtils() {
    } // not instantiable

    /**
     * Returns the given number of k-mers from {@link SequenceFile}s containing {@link KmerEntropyPairWritable} as the keys.
     *
     * @param conf      JobConf
     * @param input     path to SequenceFile
     * @param numKmers  the number of k-mers to return (if null or 0, all will be returned).
     * @return          {@link TreeSet} of sorted (see {@link KmerEntropyPairWritable} k-mers.
     * @throws java.io.IOException
     */
    public static TreeSet<String> getKmerEntropiesFromSequenceFile(JobConf conf, String input, Integer length) throws IOException {
        TreeSet<String> nmers = new TreeSet<String>();
        Path inputPath = new Path(input);
        FileSystem fs = inputPath.getFileSystem(conf);
        //Path inputPath = fs.makeQualified(path);
        Path[] paths = FileUtils.ls(conf, inputPath.toString() + Path.SEPARATOR + "part-*");
        if (length == null || length <= 0) {
            length = Integer.MAX_VALUE;
        }
        int cnt = 0;
        for (int idx = 0; idx < paths.length; idx++) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[idx], conf);
            KmerEntropyPairWritable key = new KmerEntropyPairWritable();
            boolean hasNext = true;
            while (hasNext && cnt < length) {
                hasNext = reader.next(key);
                nmers.add(key.getKey());
                cnt++;
            }
        }
        return nmers;
    }

    /**
     * Flattens a {@link SequenceFile} containing {@link KmerEntropyPairWritable}s as keys to a file
     * containing only the keys as {@link KmerEntropyPairWritable} in the same order.
     *
     * @param conf
     * @param numKmers
     * @param input     the input path containing the kmers.
     * @param output    the output file path to write the keys to.
     * @param asText    if <code>true</code>, then save keys and values as text. Otherwise, save as {@link Writable}s
     * @return          the actual number written out.
     * @throws java.io.IOException
     */
    public static synchronized int flattenKmerEntropySequenceFile(JobConf conf, int numKmers, String input, String output, boolean asText) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Flattening %d k-mers entropies from %s to %s", numKmers, input, output));
        }
        Path outPath = new Path(output);
        FileSystem fs = outPath.getFileSystem(conf);

        FSDataOutputStream fos = fs.create(outPath, true);   // throws nothing!
        Path inputPath = new Path(input);
        Path[] paths = FileUtils.ls(conf, inputPath.toString() + Path.SEPARATOR + "part-*");
        if (numKmers <= 0) {
            numKmers = Integer.MAX_VALUE;
        }
        int cnt = 0;
        for (int idx = 0; idx < paths.length; idx++) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[idx], conf);
            KmerEntropyPairWritable key = new KmerEntropyPairWritable();
            boolean hasNext = true;
            while (hasNext && cnt < numKmers) {
                hasNext = reader.next(key);
                if (asText) {
                    fos.writeUTF(key.toString());
                } else {
                    key.write(fos);
                }
                cnt++;
            }
            try {
                fos.close();
                reader.close();
            } catch (IOException ioe) {
                // closing the SequenceFile.Reader will throw an exception if the file is over some unknown size
                LOG.debug("Probably caused by closing the SequenceFile.Reader", ioe);
            }
        }
        return cnt;
    }

    /**
     * Writes out the {@link SequenceFile} feature vectors in row major (packed) order. No labels are outputed.
     *
     * @param jobConf
     * @param input     top level SequenceFile directory path
     * @param output    path to output the matrix
     * @param digits    the maximum number of fraction digits
     * @throws IOException
     */
    public static void featureVectors2RowMajorMatrix(JobConf jobConf, String input, String output, int digits) throws IOException {
        JobConf conf = new JobConf(jobConf, CalculateCosineDistanceMatrix.class);

        DecimalFormat format = new DecimalFormat();
        format.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.US));
        format.setMinimumIntegerDigits(1);
        format.setMaximumFractionDigits(digits);
        //format.setMinimumFractionDigits(fractionDigits);
        format.setGroupingUsed(false);

        final Path inputPath = new Path(input);
        final FileSystem fs = inputPath.getFileSystem(conf);
        final Path qInputPath = fs.makeQualified(inputPath);
        final Path outputPath = new Path(output);
        Path[] paths = FileUtils.ls(conf, qInputPath.toString() + Path.SEPARATOR + "part-*");

        FSDataOutputStream fos = fs.create(outputPath, true);   // throws nothing!
        final Writer writer = new OutputStreamWriter(fos);
        final Text key = new Text();
        final SparseVectorWritable value = new SparseVectorWritable();
        for (int idx = 0; idx < paths.length; idx++) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[idx], conf);
            boolean hasNext = reader.next(key, value);
            while (hasNext) {

                final SparseVector vector = value.get();
                final StringBuilder sb = new StringBuilder();
                for (int i = 0; i < vector.getCardinality(); i++) {
                    final String s = format.format(vector.get(i)); // format the number
                    sb.append(s);
                    sb.append(' ');
                }
                writer.write(sb.toString());
                hasNext = reader.next(key, value);
            }
            try {
                writer.flush();
                reader.close();
            } catch (IOException ioe) {
                // closing the SequenceFile.Reader will throw an exception if the file is over some unknown size
                LOG.debug("Probably caused by closing the SequenceFile.Reader. All is well", ioe);
            }
        }
        try {
            writer.close();
            fos.flush();
            fos.close();
        } catch (IOException ioe) {
            LOG.debug("Caused by distributed cache output stream.", ioe);
        }
    }

    /**
     * Flattens a {@link SequenceFile} containing {@link KmerEntropyPairWritable}s as keys to a json file
     * containing the k-mers (<tt>features</tt>) in the same order, along with the start and end window sizes.
     *
     * @param conf
     * @param numKmers      the number of k-mers to return (if 0 or less, all will be returned).
     * @param input         the input path containing the kmers.
     * @param output        the output file path to write the json file to.
     * @return the actual number of kmers written out
     * @throws              java.io.IOException
     */
    public static int kmerSequenceFile2Json(JobConf conf, int start, int end, int numKmers, String input, String output) throws IOException {
        Path outPath = new Path(output);
        FileSystem fs = outPath.getFileSystem(conf);

        FSDataOutputStream fos = fs.create(outPath, true);   // throws nothing!
        Path inputPath = new Path(input);
        Path[] paths = FileUtils.ls(conf, inputPath.toString() + Path.SEPARATOR + "part-*");
        if (numKmers <= 0) {
            numKmers = Integer.MAX_VALUE;
        }
        int cnt = 0;
        Writer writer = new OutputStreamWriter(fos);
        JsonFactory jf = new JsonFactory();
        JsonGenerator jg = jf.createJsonGenerator(writer);
        CompleteCompositionVectorUtils util = new CompleteCompositionVectorUtils();
        try {
            jg.writeStartObject();
            util.writeJsonCcvProperties(jg, start, end);
            cnt = util.writeJsonKmers(conf, fs, paths, jg, numKmers);
            jg.writeEndObject();
            jg.close();
            writer.close();
        } catch (JsonGenerationException ex) {
            LOG.error("Unable to write the nmers to a json object", ex);
        }
        return cnt;
    }

    /**
     * Write out feature vectors, features (k-mers), and properties (start, end) to a JSON file.
     * <P>JSON format
     * <blockquote>
     * {
     *      "properties" :
     *      {
     *          "begin" : 3
     *          "end"   : 9
     *      }
     *      "features" : [..]
     *      "samples" :
     *      [
     *          {
     *              "name" : "sample name",
     *              "data" : { nmer_index: non-zero pi-values }
     *          }, ....
     *      ]
     * }
     * </blockquote>
     *
     * The data will be the same as  {@link org.mitre.ccv.CompleteMatrix#jsonCompleteMatrix}, but the features
     * will be in a different order. The mapred version, by default sorts, only by entropy value, whereas the
     * ccv in-memory version sorts by the k-mer natural order (lexigraphic).
     *
     * @see {@link org.mitre.ccv.CompleteMatrix#jsonCompleteMatrix}
     *
     * @param conf          the job configuration
     * @param start         begining window size
     * @param end           ending window size
     * @param numKmers      the number of k-mers to return (if 0 or less, all will be returned).
     * @param listInput     {@link SequenceFile} path containing k-mers used to generate the feature vectors.
     * @param featureInput  {@link SequenceFile} path contains feature vectors {@link SparseVectorWritable}.
     * @param output        the output file path to write the json file to.
     * @return the actual number of kmers written out (not samples/feature vectors)
     * @throws java.io.IOException
     */
    public static int featureVectors2Json(JobConf conf, int start, int end,
            int numKmers, String listInput, String featureInput, String output) throws IOException {
        Path outPath = new Path(output);
        FileSystem fs = outPath.getFileSystem(conf);

        FSDataOutputStream fos = fs.create(outPath, true);   // throws nothing!
        if (numKmers <= 0) {
            numKmers = Integer.MAX_VALUE;
        }
        Writer writer = new OutputStreamWriter(fos);

        JsonFactory jf = new JsonFactory();
        JsonGenerator jg = jf.createJsonGenerator(writer);
        CompleteCompositionVectorUtils util = new CompleteCompositionVectorUtils();
        int cnt = 0;
        try {
            jg.writeStartObject();
            util.writeJsonCcvProperties(jg, start, end);

            /** Get k-mers (features) */
            Path inputPath = new Path(listInput);
            Path[] paths = FileUtils.ls(conf, inputPath.toString() + Path.SEPARATOR + "part-*");
            cnt = util.writeJsonKmers(conf, fs, paths, jg, numKmers);

            /** Get samples */
            inputPath = new Path(featureInput);
            paths = FileUtils.ls(conf, inputPath.toString() + Path.SEPARATOR + "part-*");
            util.jsonCcvVectors(conf, fs, paths, jg);
            jg.writeEndObject();
            jg.close();
            writer.close();
        } catch (JsonGenerationException ex) {
            LOG.error("Unable to write the nmers to a json object", ex);
        }
        return cnt;
    }

    /**
     * Writes a JSON array of the k-mers ("features") in order (hopefully).
     *
     * "features" : [...]
     *
     * @return the actual number of kmers written out.
     */
    private int writeJsonKmers(JobConf conf, FileSystem fs, Path[] paths, JsonGenerator jg, int numKmers) throws JsonGenerationException, IOException {
        jg.writeArrayFieldStart("features");
        int cnt = 0;
        KmerEntropyPairWritable key = new KmerEntropyPairWritable();
        for (int idx = 0; idx < paths.length; idx++) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[idx], conf);
            boolean hasNext = reader.next(key);
            while (hasNext && cnt < numKmers) {
                cnt++;
                jg.writeString(key.getKey());
                hasNext = reader.next(key);
            }
        }
        jg.writeEndArray();
        return cnt;
    }

    /**
     * "properties" : { "begin":start, "end":end }
     */
    private void writeJsonCcvProperties(JsonGenerator jg, int start, int end) throws JsonGenerationException, IOException  {
        jg.writeObjectFieldStart("properties");
        jg.writeNumberField("begin", start);
        jg.writeNumberField("end", end);
        jg.writeEndObject();
    }

    /**
     * Writes out our vectors as sparse arrays (only non-zeros) in JSONObjects in a JSONArray
     * Format:
     * "samples": [
     *      {
     *          "name" : "sample name",
     *          "data" : { nmer_index: non-zero pi-values }
     *      }, ....
     * ]
     *
     * nmer_index starts at 0 (zero)
     *
     * data is stored as SparseVectors in SequenceFiles with the key (Text) as the name.
     *
     * @param paths     listing of paths (parts-) files
     * @param map       mapping of k-mers to position
     *
     */
    private void jsonCcvVectors(JobConf conf, FileSystem fs, Path[] paths, JsonGenerator jg) throws JsonGenerationException, IOException {
        jg.writeArrayFieldStart("samples");
        Text key = new Text();
        SparseVectorWritable values = new SparseVectorWritable();
        for (int idx = 0; idx < paths.length; idx++) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[idx], conf);
            boolean hasNext = reader.next(key, values);
            while (hasNext) {
                jg.writeStartObject();
                jg.writeStringField("name", key.toString());
                jg.writeObjectFieldStart("data");
                SparseVector vector = values.get();
                for (Iterator<Entry<Integer, Double>> iter = vector.getSparseMap().entrySet().iterator(); iter.hasNext();) {
                    Entry<Integer, Double> entry = iter.next();
                    if (entry.getValue().isInfinite()) {
                        LOG.warn(String.format("Skipping %s:%d\t%f\n", key.toString(), entry.getKey(), entry.getValue()));
                        System.err.printf("Skipping %s:%d\t%f\n", key.toString(), entry.getKey(), entry.getValue());
                    } else {
                        jg.writeNumberField(Integer.toString(entry.getKey()), entry.getValue());
                    }
                    jg.flush();         // force the buffer to empty to disk/io
                }
                jg.writeEndObject();    // data object
                jg.writeEndObject();    // sample object
                hasNext = reader.next(key, values);
            }
        }
        jg.writeEndArray(); // samples array
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf());

        ArrayList<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else if ("-libjars".equals(args[i])) {
                    conf.set("tmpjars",
                            FileUtils.validateFiles(args[++i], conf));

                    URL[] libjars = FileUtils.getLibJars(conf);
                    if (libjars != null && libjars.length > 0) {
                        // Add libjars to client/tasks classpath
                        conf.setClassLoader(new URLClassLoader(libjars, conf.getClassLoader()));
                        // Adds libjars to our classpath
                        Thread.currentThread().setContextClassLoader(
                                new URLClassLoader(libjars,
                                Thread.currentThread().getContextClassLoader()));
                    }
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i - 1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() < 1) {
            System.out.println("ERROR: Require ONE argument!");
            return printUsage();
        }

        String cmd = other_args.get(0);
        if (cmd.equals("featureVectors2Json")) {
            if (other_args.size() >= 7) {
                try {
                    int start = Integer.parseInt(other_args.get(1));
                    int end = Integer.parseInt(other_args.get(2));
                    int kmers = Integer.parseInt(other_args.get(3));
                    featureVectors2Json(conf, start, end, kmers, other_args.get(4), other_args.get(5), other_args.get(6));
                } catch (NumberFormatException except) {
                    System.err.println("Woops. Error converting number!");
                    return -1;
                }
            } else {
                System.err.println("We need more arguments!");
                return -1;
            }
        } else if (cmd.equals("featureVectors2rows")) {
            int digits = 6;
            if (other_args.size() > 3 ) {
                try {
                    digits = Integer.parseInt(other_args.get(1));
                    featureVectors2RowMajorMatrix(conf, other_args.get(2), other_args.get(3), digits);
                } catch (NumberFormatException except) {
                    System.err.println("Woops. Error converting number!");
                    return -1;
                }
            } else {
                featureVectors2RowMajorMatrix(conf, other_args.get(1), other_args.get(2), digits);
            }
        } else {
            System.out.println("Unknown command:" + cmd);
            return -1;
        }
        return 0;
    }

    static int printUsage() {
        System.out.println("CompleteCompositionVectorUtils [-libjars <classpath,...>] [-m <maps>] [-r <reduces>] command [arguments]");
        System.out.println("Current commands:");
        System.out.println("\tfeatureVectors2json\tstart end num-kmers kmer-list feature-vectors output");
        System.out.println("\tfeatureVectors2rows\t[digits] feature-vectors output");
        return -1;
    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CompleteCompositionVectorUtils(), args);
        System.exit(res);
    }
}

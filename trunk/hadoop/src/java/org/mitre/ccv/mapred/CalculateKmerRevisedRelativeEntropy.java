/**
 * Created on April 1, 2009.
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.mitre.ccv.mapred.io.CompositionVectorKey;
import org.mitre.ccv.mapred.io.CompositionVectorWritable;
import org.mitre.ccv.mapred.io.KmerEntropyPairWritable;
import org.mitre.mapred.io.StringDoublePairWritable;
import org.mitre.mapred.fs.FileUtils;

/**
 * Map-reduce class that calculates the Revised Relative Entropy (RRE).
 *
 * <P>This expects the output of the global pi-values ({@link CalculateKmerPiValues})
 * to be {@link Text} as the key and {@link StringDoublePairWritable}
 * with global being set as values.
 *
 * <p>This supports two different output formats
 * <ul>
 * <li>Binary SequenceFile (default)</li>
 * <li>plain text (-t option or setting the binary JobConf property {@link #TEXT_OUTPUT})
 * </ul>
 *
 * @author Marc Colosimo
 */
public class CalculateKmerRevisedRelativeEntropy extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(CalculateKmerRevisedRelativeEntropy.class);

    public static final String COMPOSITION_VECTORS_KMER_POSTFIX = "_cvkmers";

    public static final String MERGED_KMER_POSTFIX = "_mergedkmers";

    public static final String TEXT_OUTPUT = "ccv.rre.textoutput";

    /**
     * A {@link Writable} class for collecting all the k-mer/value pairs keeping track
     * of which ones are global (whole corpus) or per-sample (sequence).
     */
    public static class KmerPiValueArrayWritable implements Writable {

        ArrayList<StringDoublePairWritable> list = new ArrayList<StringDoublePairWritable>();
        int globalIdx = -1;

        public KmerPiValueArrayWritable() {
        }

        public KmerPiValueArrayWritable(StringDoublePairWritable w) {
            this.add(w);
        }

        public void add(StringDoublePairWritable w) {
            if (!w.isLocal()) {
                this.globalIdx = this.list.size();
            }
            this.list.add(w);
        }

        public StringDoublePairWritable getGlobalKmerPiValue() {
            if (this.globalIdx >= 0 && this.globalIdx < this.list.size()) {
                return this.list.get(this.globalIdx);
            }
            return null;
        }

        public void clear() {
            this.list.clear();
            this.globalIdx = -1;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.list.size());
            out.writeInt(this.globalIdx);
            for (int cv = 0; cv < this.list.size(); cv++) {
                this.list.get(cv).write(out);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int size = in.readInt();
            this.globalIdx = in.readInt();
            this.list.clear();
            for (int cv = 0; cv < size; cv++) {
                StringDoublePairWritable w = new StringDoublePairWritable();
                w.readFields(in);
                this.list.add(w);
            }
        }
    }

    /**
     * Mapper that maps the k-mer pi-values from composition vectors to {@link StringDoublePairWritable}s
     */
    public static class CompositionVectorMap extends MapReduceBase
            implements Mapper<CompositionVectorKey, CompositionVectorWritable, Text, StringDoublePairWritable> {

        private Text kmer = new Text();
        private StringDoublePairWritable pi = new StringDoublePairWritable();

        @Override
        public void map(CompositionVectorKey key, CompositionVectorWritable cv,
                OutputCollector<Text, StringDoublePairWritable> output,
                Reporter reporter) throws IOException {
            reporter.setStatus("Working on " + key.toString());
            //System.err.println(key.toString());
            for (Iterator<Entry<String, Double>> iter =
                    cv.getCompositionVector().entrySet().iterator(); iter.hasNext();) {
                Entry<String, Double> entry = iter.next();
                kmer.set(entry.getKey());
                pi.set(entry.getKey(), entry.getValue());
                //System.err.printf("\t%s=%f\n", entry.getKey(), entry.getValue());
                output.collect(kmer, pi);
            }
        }
    }

    /**
     * Mapper that emits each StringDoublePairWritable in a new KmerPiValueArrayWritable object.
     */
    public static class MergeMap extends MapReduceBase
            implements Mapper<Text, StringDoublePairWritable, Text, KmerPiValueArrayWritable> {

        @Override
        public void map(Text kmer, StringDoublePairWritable value,
                OutputCollector<Text, KmerPiValueArrayWritable> output,
                Reporter reporter) throws IOException {
            output.collect(kmer, new KmerPiValueArrayWritable(value));
        }
    }

    /**
     * Combiner/Reducer for merging together local and <b>A</b> global k-mer/pi-values
     * into one KmerPiValueArrayWritable object.
     */
    public static class MergeReducer extends MapReduceBase
            implements Reducer<Text, KmerPiValueArrayWritable, Text, KmerPiValueArrayWritable> {

        @Override
        public void reduce(Text kmer, Iterator<KmerPiValueArrayWritable> values,
                OutputCollector<Text, KmerPiValueArrayWritable> output,
                Reporter reporter) throws IOException {

            reporter.setStatus("Merging " + kmer.toString());
            //System.err.printf("Merging %s\n", kmer.toString());
            KmerPiValueArrayWritable aw = new KmerPiValueArrayWritable();
            while (values.hasNext()) {
                KmerPiValueArrayWritable w = values.next();
                for (Iterator<StringDoublePairWritable> iter = w.list.iterator(); iter.hasNext();) {
                    StringDoublePairWritable vpw = iter.next();
                    //aw.add(iter.next());
                    aw.add(vpw);
                //System.err.printf("\t%s=%f\tl=%b\n", vpw.getKmer(), vpw.getValue(),vpw.isLocal() );
                }
            }
            output.collect(kmer, aw);
        }
    }

    /**
     * Mapper for first step in calculating RRE.
     */
    public static class EntropyMap extends MapReduceBase
            implements Mapper<Text, KmerPiValueArrayWritable, Text, KmerEntropyPairWritable> {

        KmerEntropyPairWritable ent = new KmerEntropyPairWritable();

        @Override
        public void map(Text kmer, KmerPiValueArrayWritable value,
                OutputCollector<Text, KmerEntropyPairWritable> output,
                Reporter reporter) throws IOException {
            reporter.setStatus("Working on " + kmer.toString());
            //System.out.print(kmer.toString());
            StringDoublePairWritable gPi = value.getGlobalKmerPiValue();
            if (gPi == null) {
                throw new IOException("Missing global pi-value for " + kmer.toString());
            }
            for (Iterator<StringDoublePairWritable> iter = value.list.iterator(); iter.hasNext();) {
                StringDoublePairWritable w = iter.next();
                if (w.isLocal() && w.getValue() != 0) {
                    // DO NOT DO GLOBAL AGAINST GLOBAL
                    double e = Math.abs(w.getValue()) *
                            Math.log(Math.abs(w.getValue() / gPi.getValue()));
                    ent.set(kmer.toString(), e);
                    //System.out.printf("\tl=%f\tg=%f\te=%f\n",w.getValue(), gPi.getValue(), e );
                    output.collect(kmer, ent);
                }
            }
        }
    }

    /**
     * Combiner for summing mapped entropy values.
     */
    public static class EntropyCombiner extends MapReduceBase
            implements Reducer<Text, KmerEntropyPairWritable, Text, KmerEntropyPairWritable> {

        @Override
        public void reduce(Text kmer, Iterator<KmerEntropyPairWritable> values,
                OutputCollector<Text, KmerEntropyPairWritable> output,
                Reporter reporter) throws IOException {

            reporter.setStatus("Summing " + kmer.toString());
            double sum = 0.0;
            while (values.hasNext()) {
                sum += values.next().getValue();
            }
            // DO NOT TAKE Math.abs HERE!
            output.collect(kmer, new KmerEntropyPairWritable(kmer.toString(), sum));
        }
    }

    /**
     * Reducer for summing maped values and returning the abs value.
     * 
     * <P>This <B>CANNOT</B> be used as a Combiner!
     */
    public static class EntropyReducer extends MapReduceBase
            implements Reducer<Text, KmerEntropyPairWritable, Text, KmerEntropyPairWritable> {

        @Override
        public void reduce(Text kmer, Iterator<KmerEntropyPairWritable> values,
                OutputCollector<Text, KmerEntropyPairWritable> output,
                Reporter reporter) throws IOException {

            reporter.setStatus("Summing " + kmer.toString());
            double sum = 0.0;
            while (values.hasNext()) {
                sum += values.next().getValue();
            }
            output.collect(kmer, new KmerEntropyPairWritable(kmer.toString(), Math.abs(sum)));
        }
    }

    /**
     * Start up a map-reduce job with the given parameters.
     *
     * @param jobConf
     * @param start     starting window size
     * @param end       ending window size
     * @param input
     * @param output
     * @return
     * @throws java.lang.Exception
     */
    public int initJob(JobConf jobConf, String globalInput, String cvInput,
            String output, boolean cleanLogs) throws Exception {
        JobConf conf = new JobConf(jobConf, CalculateKmerRevisedRelativeEntropy.class);
        conf.setJobName("CalculateKmerRevisedRelativeEntropy");

        /** 
         * Set up paths
         */
        String ts = FileUtils.getSimpleDate();
        String cvOutput = output + "_" + ts + COMPOSITION_VECTORS_KMER_POSTFIX;

        /** commaSeparatedPaths */
        String mergedInput = cvOutput + "," + globalInput;

        /** merged output */
        String mergedOutput = output + "_" + ts + MERGED_KMER_POSTFIX;

        /**
         * First, map all the CompositionVector's k-mers to Text as keys and
         * local k-mer/value pairs (KmerPiValuePairWritables) as values.
         */
        JobConf subConf = new JobConf(conf);
        subConf.setJobName("CalculateKmerRevisedRelativeEntropy-CompositionVectors");
        // setup mapper
        SequenceFileInputFormat.setInputPaths(subConf, cvInput);
        subConf.setInputFormat(SequenceFileInputFormat.class);
        subConf.setMapperClass(CompositionVectorMap.class);
        subConf.setOutputKeyClass(Text.class);         // job output key class
        subConf.setOutputValueClass(StringDoublePairWritable.class);  // job output value class

        // Uses default reducer (IdentityReducer)
        subConf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(subConf, new Path(cvOutput));
        LOG.info("Converting CompositionVectors to k-mer/pi-value pairs.");
        JobClient.runJob(subConf);

        /**
         * Second, map (merge) all the k-mer/pi-value pairs together in an
         * array of values (KmerPiValueArrayWritables).
         */
        subConf = new JobConf(conf);
        subConf.setJobName("CalculateKmerRevisedRelativeEntropy-Merging");
        // setup mapper
        SequenceFileInputFormat.setInputPaths(subConf, mergedInput);
        subConf.setInputFormat(SequenceFileInputFormat.class);
        subConf.setMapperClass(MergeMap.class);
        subConf.setOutputKeyClass(Text.class);
        subConf.setOutputValueClass(KmerPiValueArrayWritable.class);

        // setup combiner/reducer
        subConf.setCombinerClass(MergeReducer.class);
        subConf.setReducerClass(MergeReducer.class);
        subConf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(subConf, new Path(mergedOutput));
        LOG.info("Merging k-mers/pi-values from CompositionVectors and all sequences (global)");
        JobClient.runJob(subConf);

        /**
         * Third, calculate entropies (map-reduce)
         */
        subConf = new JobConf(conf);
        subConf.setJobName("CalculateKmerRevisedRelativeEntropy-RRE");
        // setup mapper
        SequenceFileInputFormat.setInputPaths(subConf, mergedOutput);
        subConf.setInputFormat(SequenceFileInputFormat.class);
        subConf.setMapperClass(EntropyMap.class);
        subConf.setOutputKeyClass(Text.class);
        subConf.setOutputValueClass(KmerEntropyPairWritable.class);

        // Setup Combiner and Reducer
        subConf.setCombinerClass(EntropyCombiner.class);
        subConf.setReducerClass(EntropyReducer.class);
        if (conf.getBoolean(TEXT_OUTPUT, false)) {
            FileOutputFormat.setOutputPath(subConf, new Path(output));
        } else {
            subConf.setOutputFormat(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(subConf, new Path(output));
        }
        
        LOG.info("Calculating entropies");
        JobClient.runJob(subConf);

        /**
         * Remove tmp directories
         */
        Path tmp = new Path(cvOutput);
        FileSystem fs = tmp.getFileSystem(conf);
        fs.delete(tmp, true);
        tmp = new Path(mergedOutput);
        fs.delete(tmp, true);

        return 0;
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf());
        boolean cleanLogs = false;

        // @TODO: use commons getopts
        List<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else if ("-c".equals(args[i])) {
                    cleanLogs = true;
                } else if ("-t".equals(args[i])) {
                    conf.setBoolean(TEXT_OUTPUT, true);
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
        if (other_args.size() != 3) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 3.");
            return printUsage();
        }

        //return initJob(conf, inTable, sb.toString().trim(), new Path(other_args.get(1)));
        return initJob(conf, other_args.get(0), other_args.get(1), other_args.get(2), cleanLogs);

    }

    static int printUsage() {
        System.out.println("CalculateKmerRevisedRelativeEntropy [-libjars <classpath,...>] [-m <maps>] [-r <reduces>]" +
                "[-t] <k-mer pi-values input> <composition vectors input> <output>");
        System.out.println("Where -t sets the output to be text instead of the default binary SequenceFile.");
        return -1;
    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CalculateKmerRevisedRelativeEntropy(), args);
        System.exit(res);
    }
}

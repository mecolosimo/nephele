/**
 * Created on March 25, 2009.
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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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

import org.mitre.mapred.fs.FileUtils;
import org.mitre.ccv.mapred.io.KmerCountWritable;

/**
 * Map-reduce class that calculates the counts of k-mers (n-grams) from all of the sequences.
 * <p>This generates k-mers for start - 2 to end.</p>
 *
 * By setting the system property (-D) "kmer.count.parent.fast.map" to "true" (or "T") this will use an internal {@link java.util.Map}.
 * Note that the MapTask might run out of memory.
 * 
 * @author Marc Colosimo
 */
public class CalculateKmerCounts extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(CalculateKmerCounts.class);
    public static final int DEFAULT_START = 3;
    public static final int DEFAULT_END = 9;
    public static final String START = "kmer.window.start.size";
    public static final String END = "kmer.window.end.size";
    public static final String LENGTH = "total.sequence.length";
    public static final String FAST_MAP = "kmer.count.parent.fast.map";

    /**
     * Mapper that generate the k-mer counts and emits their parents.
     *
     * <P>If <code>FAST_MAP</code> is set then this will use a HashMap to limit
     * the childern of a k-mer at the expense of local memory.</P>
     */
    public static class KmerCountMap extends MapReduceBase
            implements Mapper<Text, Text, Text, KmerCountWritable> {

        private int start;
        private int end;
        private Text kmerText = new Text();
        private KmerCountWritable kmerCnts = new KmerCountWritable();
        private HashMap<String, Integer> kmap = null;
        private Integer kmapValue = new Integer(1);

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            start = Integer.parseInt(job.get(START));
            end = Integer.parseInt(job.get(END));

            String fast = job.get(FAST_MAP,"F");
            if (fast.equalsIgnoreCase("T") || fast.equalsIgnoreCase("true")) {
                kmap = new HashMap<String, Integer>();
                LOG.info("Using fast maps");
            } else {
                kmap = null;
            }
            LOG.debug("Configure: " + job);
        }

        /**
         * Generates kmer counts and their parents between the given start/end.
         * 
         * @param key           Sequence name
         * @param value         The sequence
         * @param output        output collector
         * @param reporter      reporter
         * @throws java.io.IOException
         */
        @Override
        public void map(Text key, Text value,
                OutputCollector<Text, KmerCountWritable> output,
                Reporter reporter) throws IOException {

            String seq = value.toString();

            /**
             * CalculateCompostionVectors checks the sizes (actually IndexedCompositionDistribution does)
             */
            if( this.end > seq.length() ) {
                LOG.info(String.format("%s length (%d) is smaller than the end window size (%d). Skipping..",
                        key.toString(), seq.length(), this.end));
                return;
            }
            for (int windowSize = this.start - 2; windowSize <= this.end; windowSize++) {
                reporter.setStatus(String.format("%s: window size of %d\n", key.toString(), windowSize));
                //System.err.printf("%s: window size of %d\n", key.toString(), windowSize);
                for (int i = 0; i < seq.length() - windowSize + 1; i++) {
                    String kmer = seq.substring(i, i + windowSize);
                    kmerText.set(kmer);
                    kmerCnts.set(kmer, 1);
                    output.collect(kmerText, kmerCnts);

                    // now check to see if this can be a parent
                    if (kmer.length() >= this.start) {

                        if (this.kmap != null && this.kmap.containsKey(kmer)) {
                            //System.out.println("Seen: " + kmer);
                            continue;
                        }
                        if (this.kmap != null) {
                            this.kmap.put(kmer, kmapValue);
                            //System.out.println("New: " + kmer);
                        }

                        // Get a1..ak-1
                        String subst = kmer.substring(0, kmer.length() - 1);

                        kmerText.set(subst);
                        kmerCnts.set(subst);            // count of zero !
                        kmerCnts.addParent(kmer);
                        output.collect(kmerText, kmerCnts);

                        // Get a2..ak
                        subst = kmer.substring(1, kmer.length());
                        kmerText.set(subst);
                        kmerCnts.set(subst);            // count of zero !
                        kmerCnts.addParent(kmer);
                        output.collect(kmerText, kmerCnts);

                        // Get a2..ak-1
                        subst = kmer.substring(1, kmer.length() - 1);
                        kmerText.set(subst);
                        kmerCnts.set(subst);            // count of zero !
                        kmerCnts.addParent(kmer);
                        output.collect(kmerText, kmerCnts);
                    }
                }
            }
        }
    }

    /**
     * Both a combiner and reducer for summing the counts and combining the parents.
     */
    public static class KmerCountReducer extends MapReduceBase
            implements Reducer<Text, KmerCountWritable, Text, KmerCountWritable> {

        @Override
        public void reduce(Text key,
                Iterator<KmerCountWritable> values,
                OutputCollector<Text, KmerCountWritable> output,
                Reporter reporter) throws IOException {
            int sum = 0;
            KmerCountWritable kcnt;
            HashSet<String> parents = new HashSet<String>();
            while (values.hasNext()) {
                kcnt = values.next();
                sum += kcnt.getCount();
                parents.addAll(kcnt.getParents());
            }
            kcnt = new KmerCountWritable(key.toString(), sum);
            kcnt.addParent(parents);
            //System.err.printf("kmer=%s\tcount=%d\tnumParents=%d", key.toString(), sum, parents.size());
            output.collect(key, kcnt);
        }
    }

    /**
     * Start up a map-reduce job with the given parameters.
     *
     * <P>Setting the system property "kmer.count.parent.fast.map" will result in this using a {@link java.util.Map}
     * to speed up the output of kmers at the expense of memory.
     *
     * @param jobConf
     * @param start     starting window size
     * @param end       ending window size
     * @param input
     * @param output
     * @return
     * @throws java.lang.Exception
     */
    public int initJob(JobConf jobConf, int start, int end,
            String input, String output) throws Exception {
        JobConf conf = new JobConf(jobConf, CalculateKmerCounts.class);
        conf.setJobName("CalculateKmerCounts");

        if( start <= 2 )
            throw new IllegalArgumentException("Value of 'start' argument must be larger than 2");


        // Save our window size so that the tasks have access to them
        conf.set(START, Integer.toString(start));
        conf.set(END, Integer.toString(end));
        //conf.set(FAST_MAP, fastMap ? "Y":"N");

        // Set up mapper
        SequenceFileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setMapperClass(KmerCountMap.class);
        conf.setOutputKeyClass(Text.class);                 // map output key class
        conf.setOutputValueClass(KmerCountWritable.class);  // map output value class

        // Set up combiner/reducer
        conf.setCombinerClass(KmerCountReducer.class);
        conf.setReducerClass(KmerCountReducer.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);

        return 0;
    }

    static int printUsage() {
        System.out.println("CalculateKmerCounts [-libjars <classpath,...>] [-m <maps>] [-r <reduces>]  [-f] [-s <start>] [-e <end>] <input> <output>");
        System.out.printf("\twhere the default start=%d and end=%d\n", DEFAULT_START, DEFAULT_END);
        System.out.println("\t-f will use a HashMap on the mappers to limit the number of k-mers the reducers have to sort through");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf());
        int start = DEFAULT_START;
        int end = DEFAULT_END;

        // @TODO: use commons getopts
        List<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else if ("-s".equals(args[i])) {
                    start = Integer.parseInt(args[++i]);
                } else if ("-e".equals(args[i])) {
                    end = Integer.parseInt(args[++i]);
                } else if ("-f".equals(args[i])) {
                    conf.get(FAST_MAP, "true");
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
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");

            return printUsage();
        }

        return initJob(conf, start, end, other_args.get(0), other_args.get(1));

    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CalculateKmerCounts(), args);
        System.exit(res);
    }
}

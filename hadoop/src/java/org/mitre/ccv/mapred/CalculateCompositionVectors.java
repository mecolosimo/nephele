/**
 * Created on March 30, 2009.
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
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.mitre.ccv.CompositionVector;
import org.mitre.ccv.index.IndexedCompleteCompositionVector;
import org.mitre.ccv.index.IndexedCompositionDistribution;
import org.mitre.ccv.mapred.io.CompositionVectorKey;
import org.mitre.ccv.mapred.io.CompositionVectorWritable;

import org.mitre.mapred.fs.FileUtils;

/**
 * A map-reduce class that calaculates the composition vectors for sequences in {@link SequenceFiles}.
 *
 * <P>By default this generates the complete composition vectors by generating
 * composition vectors over {@link CalculateKmerCounts#DEFAULT_START} to
 * {@link CalculateKmerCounts#DEFAULT_END} window sizes</P>
 *
 * @bug: this doesn't seem to partition correctly. For win32, all went to one partitioner! (fixed? CompositionVectorKey, recheck)
 * 
 * @author Marc Colosimo
 */
public class CalculateCompositionVectors extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(CalculateCompositionVectors.class);

    public static class CompositionVectorMap extends MapReduceBase
            implements Mapper<Text, Text, CompositionVectorKey, CompositionVectorWritable> {

        private static final Log MAP_LOG = LogFactory.getLog(CompositionVectorMap.class);
        private int start;
        private int end;
        private CompositionVectorKey cvKey = new CompositionVectorKey();

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            start = Integer.parseInt(job.get(CalculateKmerCounts.START));
            end = Integer.parseInt(job.get(CalculateKmerCounts.END));
            MAP_LOG.debug("Configured: " + job);
        }

        /**
         *
         * @param key       Sample name
         * @param value     Sample sequence
         * @param output
         * @param reporter
         * @throws java.io.IOException
         */
        @Override
        public void map(Text key, Text value,
                OutputCollector<CompositionVectorKey, CompositionVectorWritable> output,
                Reporter reporter) throws IOException {

            String seq = value.toString();

            /**
             * IndexedCompositionDistribution will throw IllegalArgumentException
             */
            if( this.end > seq.length() ) {
                LOG.info(String.format("%s length (%d) is smaller than the end window size (%d). Skipping..",
                        key.toString(), seq.length(), this.end));
                return;
            }

            reporter.setStatus(String.format("Generating distribution for %s (%d to %d)",
                    key.toString(), this.start, this.end));
            //System.out.printf("Generating distribution for %s (%d to %d)",
            //        key.toString(), this.start, this.end);
            IndexedCompositionDistribution cd =
                    new IndexedCompositionDistribution(null, 1, seq,
                    this.start, this.end);

            reporter.setStatus(String.format("Generating pi-values for %s", key.toString()));
            IndexedCompleteCompositionVector ccv =
                    new IndexedCompleteCompositionVector(key.toString(), 1,
                    this.start, this.end, cd);
            for (int ws = this.start; ws <= this.end; ws++) {
                //System.out.printf("Generating  k-mers for windowSize %d\n", ws);
                cvKey.set(key.toString(), ws);
                CompositionVector cv = ccv.getCompositionVector(ws);
                CompositionVectorWritable cvw =
                        new CompositionVectorWritable(key.toString(), ws,
                        cv.getCompositionVector());
                //Map<String, Double> m = cvw.getCompositionVector();
                //System.out.printf("Generated %d k-mers for windowSize %d\n", m.size(), ws);
                output.collect(cvKey, cvw);
            }
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
    public int initJob(JobConf jobConf, int start, int end,
            String input, String output, boolean cleanLogs) throws Exception {
        JobConf conf = new JobConf(jobConf, CalculateKmerCounts.class);
        conf.setJobName("CalculateCompositionVectors");

        if( start <= 2 )
            throw new IllegalArgumentException("Value of 'start' argument must be larger than 2");

        // Save our window size so that the tasks have access to them
        conf.set(CalculateKmerCounts.START, Integer.toString(start));
        conf.set(CalculateKmerCounts.END, Integer.toString(end));

        // Set up mapper
        SequenceFileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setMapperClass(CompositionVectorMap.class);
        conf.setOutputKeyClass(CompositionVectorKey.class);         // job output key class
        conf.setOutputValueClass(CompositionVectorWritable.class);  // job output value class

        // Uses default reducer (IdentityReducer)
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);

        return 0;
    }

    static int printUsage() {
        System.out.println("CalculateCompositionVectors [-libjars <classpath,..>] [-m <maps>] [-r <reduces>]  -s <start> -e <end> [-c] <input> <output>");
        System.out.printf("\twhere the default start=%d and end=%d and c is to clean logs when done (default off).\n",
                CalculateKmerCounts.DEFAULT_START,
                CalculateKmerCounts.DEFAULT_END);
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf());
        int start = CalculateKmerCounts.DEFAULT_START;
        int end = CalculateKmerCounts.DEFAULT_END;
        boolean cleanLogs = false;

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
                } else if ("-c".equals(args[i])) {
                    cleanLogs = true;
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
        return initJob(conf, start, end, other_args.get(0), other_args.get(1), cleanLogs );
    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CalculateCompositionVectors(), args);
        System.exit(res);
    }

}

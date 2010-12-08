/**
 * Created on March 27, 2009.
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
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

import org.mitre.mapred.io.StringDoublePairWritable;

import org.mitre.mapred.fs.FileUtils;
import org.mitre.ccv.mapred.io.KmerProbabilityMapWritable;


/**
 * Map-reduce class for calculating the k-mer (n-gram) pi-values from the probabilities
 * generated by {@link CalculateKmerProbabilities} and processed by {@link InvertKmerProbabilities}.
 *
 * @author Marc Colosimo
 */
public class CalculateKmerPiValues extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(CalculateKmerPiValues.class);

    /**
     * Mapper for calculating the Pi-value of a k-mer.
     */
    public static class PiMapper extends MapReduceBase
            implements Mapper<Text, KmerProbabilityMapWritable, Text, StringDoublePairWritable> {

        private static final Log MAP_LOG = LogFactory.getLog(PiMapper.class);
        private StringDoublePairWritable pi = new StringDoublePairWritable();

        public int start = CalculateKmerCounts.DEFAULT_START;
        public int end = CalculateKmerCounts.DEFAULT_END;

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            start = Integer.parseInt(job.get(CalculateKmerCounts.START));
            end = Integer.parseInt(job.get(CalculateKmerCounts.END));
            LOG.debug("Configure: " + job);
        }

        public double getProb(Map<String, Double> map, String key) throws NoSuchElementException {
            if (!map.containsKey(key)) {
                throw new NoSuchElementException(key);
            }
            return map.get(key);
        }

        @Override
        public void map(Text key, KmerProbabilityMapWritable value,
                OutputCollector<Text, StringDoublePairWritable> output,
                Reporter reporter) throws IOException {

            String kmer = key.toString();
            int windowSize = kmer.length();
            if (windowSize < start) {
                //MAP_LOG.info("Skipping '" + kmer + "'. Length is to small.");
                return;
            }
            /**
             * We cannot check the length of the map because of things like 'AAA',
             * which only has 2 subsequences: 'A' and 'AA'.
             */
            // This is quick and we generate a lot of status messages !
            //reporter.setStatus("Calculating pi-value for " + kmer);
            Map<String, Double> map = value.get();
            try {
                double p = this.getProb(map, kmer);
                double p1 = this.getProb(map, kmer.substring(0, windowSize - 1));
                double p2 = this.getProb(map, kmer.substring(1, windowSize));
                double p3 = this.getProb(map, kmer.substring(1, windowSize - 1));
                double pe = (p1 * p2) / p3;
                pi.set( kmer, (p - pe) / pe, false);             // set as global
                output.collect(key, pi);
            } catch (NoSuchElementException nsee) {
                MAP_LOG.fatal("Missing k-mer for " + kmer, nsee);
               throw new IOException(String.format("Missing kmer for '%s'.\n%s",
                        key.toString(), nsee.getMessage()));
            }
        }
    }

    /**
     * Start up the job with the given parameters.
     *
     * @param jobConf       {@link JobConf} to use
     * @param start         starting window size
     * @param end           ending window size
     * @param input         path to the {@link SequenceFile}s
     * @param output        path to save the output
     * @param cleanLogs     if <code>true</code> remove the logs
     * @return exit status (0 if successful)
     * @throws java.lang.Exception
     *
     * @Todo: add the ability to just output as Doubles (add toString for StringDoublePairWritable)
     */
    public int initJob(JobConf jobConf, int start, int end, String input, String output,
            boolean cleanLogs ) throws Exception {
        JobConf conf = new JobConf(jobConf, CalculateKmerPiValues.class);
        conf.setJobName("CalculateKmerPiValues");

        conf.set(CalculateKmerCounts.START, Integer.toString(start));
        conf.set(CalculateKmerCounts.END, Integer.toString(end));

        // Set up mapper
        SequenceFileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setMapperClass(PiMapper.class);
        conf.setOutputKeyClass(Text.class);                        // final output key class
        conf.setOutputValueClass(StringDoublePairWritable.class);             // final output value class

        // Uses default reducer (IdentityReducer)
        // @TODO: it would be nice to controll if we output to TextFile,
        // however StringDoublePairWritable outputs both k-mer and value
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);

        if (cleanLogs) {
            LOG.info("removing log directory");
            Path path = new Path(output,"_logs");
            FileSystem fs = path.getFileSystem(jobConf);
            fs.delete(path, true);
        }
        return 0;
    }

    static int printUsage() {
        System.out.println("CalculateKmerPiValues [-libjars <classpath,...>] [-m <maps>] [-r <reduces>] [-c] [-s <start>] [-e <end>] <input> <output>");
        return -1;
    }


    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf());
        boolean cleanLogs = false;
        Integer start = CalculateKmerCounts.DEFAULT_START;
        Integer end = CalculateKmerCounts.DEFAULT_END;

        List<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else if ("-c".equals(args[i])) {
                    cleanLogs = true;
                } else if ("-s".equals(args[i])) {
                    start = Integer.parseInt(args[++i]);
                } else if ("-e".equals(args[i])) {
                    end = Integer.parseInt(args[++i]);
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


        return initJob(conf, start, end, other_args.get(0), other_args.get(1), cleanLogs);
    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CalculateKmerPiValues(), args);
        System.exit(res);
    }
}

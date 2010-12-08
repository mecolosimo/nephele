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
package org.mitre.bio.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.mitre.bio.mapred.io.FastaInputFormat;

/**
 * A map-reduce class that maps sequences (strings) from a FASTA formated file into a {@link SequenceFile} with {@link Text} for both key and values.
 *
 * <p>The key contains the user defined FASTA header and the sequence is the value.
 * 
 * @author Marc Colosimo
 */
public class Fasta2SequenceFile extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(Fasta2SequenceFile.class);
    private static final String HEADER_FORMAT = "bio.fasta.header.format";

    /**
     * Maps each FASTA sequence parsing the header for the key {@link Text} and the sequence
     * for the value {@link Text} into a {@link SequenceFile}.
     *
     * <P>By default this return the full description as the name</P>
     */
    public static class FastaMapper extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        private static final Log MAP_LOG = LogFactory.getLog(FastaMapper.class);
        private Text seq = new Text();
        private Text name = new Text();
        private int headerFormat;
        private String jobName = "";

        @Override
        public void configure(JobConf conf) {
            this.headerFormat = conf.getInt(HEADER_FORMAT, 0);
            this.jobName = conf.getJobName();
        }

        /**
         * Returns the first part (before any pipe characters '|') without the '>'. If no pipes, 
         * then the whole string is return.
         */
        public String getName(String header) {
            int offset = header.startsWith(">") ? 1:0;
            int index = header.indexOf("|");
            if (index > 1) {
                return header.substring(offset, index);
            } else {
                return header.substring(offset, header.length());
            }
        }

        /**
         * Expects the FASTA header (with '>') and separates name from description (NCBI style headers).
         *
         * @see {@link org.mitre.bio.io.FastaIterator}
         * @param header
         * @return
         */
        public String getSequenceName(String header) {            
            /**
             * Split apart sample name and description
             */
            int endname = header.indexOf(" ");
            String sn = (endname == -1) ? header : header.substring(0, endname);

            /**
             * Grab the part that the user wants
             */
            switch (headerFormat) {
                case 2:
                    sn = header;
                case 1:
                    sn = getName(sn);
                    break;
                case 0:
                default:
                    sn = sn.substring(header.startsWith(">") ? 1:0);
            }
            if (sn == null || sn.length() == 0) {
                // how do we get our node name/task id?
                sn = "[No Name " + Long.toHexString(System.currentTimeMillis()) + "]";
            }
            return sn.trim();
        }

        /**
         * Test mapping fasta names to count first matching (*.)\.*.
         * @param key       file position
         * @param value     FASTA record (expecting only one).
         * @param output    Sequence name parsed from header
         *                  (nothing after and "|") as the row key and the sequence in the given "family:column"
         * @param reporter
         * @throws java.io.IOException
         */
        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            /** Simple FASTA Parser - should use CCV's FastaIterator incase we get more than one sequence. **/
            BufferedReader buffer = new BufferedReader(new StringReader(value.toString()));
            String header = buffer.readLine().trim();
            if (header.startsWith(">")) {
                name.set(this.getSequenceName(header));
                reporter.setStatus("Recieved " + name);
                String line = buffer.readLine();
                StringBuffer sb = new StringBuffer();
                while (line != null) {
                    sb.append(line.trim());
                    line = buffer.readLine();
                }
                if (sb.length() == 0) {
                    MAP_LOG.warn(name + " contains no sequence!");
                } else {
                    seq.set(sb.toString());
                    output.collect(name, seq);
                }
            }
        }
    }

    /**
     * Init the job with the given parameters and run it.
     *
     * @param jobConf   the hadoop job configuration
     * @param input     input fasta file path
     * @param output    output {@link SequenceFile} path
     * @return          zero if successful
     * @throws java.lang.Exception
     */
    public int initJob(JobConf jobConf, String input, String output, boolean cleanLogs) throws Exception {
        JobConf conf = new JobConf(jobConf, Fasta2SequenceFile.class);
        conf.setJobName("Fasta2SequenceFile");

        // Set Input Format to use FastaInputFormat
        FastaInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(FastaInputFormat.class);
        conf.setMapperClass(FastaMapper.class);
        conf.setOutputKeyClass(Text.class);         // map output key class
        conf.setOutputValueClass(Text.class);       // map output value class

        // Set Output Format to use SequenceFile
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);

        if (cleanLogs) {
            LOG.info("removing log directory");
            Path path = new Path(output, "_logs");
            FileSystem fs = path.getFileSystem(jobConf);
            fs.delete(path, true);
        }
        return 0;
    }

    static int printUsage() {
        System.out.println("Fasta2SequenceFile [-m <maps>] [-r <reduces>] [-c] [-n number] <fasta input [dir]> <output dir>");
        System.out.println("\twhere\n'-c' will clean (remove) the logs when done.\n" +
                "'-n' sets how to parse the header for the sample's name:\n\t0(default) is part before any spaces." +
                "\n\t1 is 0 plus anything before a pipe character." +
                "\n\t2 is the whole header unprocessed.");
        return -1;
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf());
        boolean cleanLogs = false;

        List<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else if ("-c".equals(args[i])) {
                    cleanLogs = true;
                } else if ("-n".equals(args[i])) {
                    conf.setInt(HEADER_FORMAT, Integer.parseInt(args[++i]));
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

        return initJob(conf, other_args.get(0), other_args.get(1), cleanLogs);
    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new Fasta2SequenceFile(), args);
        System.exit(res);
    }
}

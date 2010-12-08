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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.mitre.mapred.fs.FileUtils;

/**
 * Map-Reduce class to calculate the total length of all of the sequences ({@link Strings}) in a {@link SequenceFile}.
 * 
 * <P>This expects the {@link SequenceFile} to have the types {@link Text} for both key and values</P>
 *
 * @author Marc Colosimo
 */
public class TotalSequenceLength extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(TotalSequenceLength.class);

    private static final IntWritable outputKey = new IntWritable(-1);
    public static class SequenceMapClass extends MapReduceBase
            implements Mapper<Text, Text, IntWritable, IntWritable> {

        @Override
        public void map(Text key, Text value,
                OutputCollector<IntWritable, IntWritable> output,
                Reporter reporter) throws IOException {
            reporter.setStatus("Calculating the length of sequences in " + key.toString() + "...");
            output.collect(TotalSequenceLength.outputKey, new IntWritable(value.getLength()));
        }
    }

    /**
     * Both a combiner and reducer for summing the lengths. This expects only one key value.
     */
    public static class LengthReduceClass extends MapReduceBase
            implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        public void reduce(IntWritable key, Iterator<IntWritable> values,
                OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public int getCount(JobConf jobConf, String pathString) throws IOException {
        // assume we only have one part (part-00000)
        Path outputPath = new Path(pathString,"part-00000");
        FileSystem fs = outputPath.getFileSystem(jobConf);
        InputStream in = fs.open(outputPath);
        String sin = FileUtils.convertStreamToString(in);
        String[] lines = sin.split("\n");
        String cnt = lines[0].split("\t")[1].trim();
        return Integer.parseInt(cnt);
    }

    /**
     * Init the job with the given parameters and run it.
     *
     * @param jobConf   the hadoop job configuration
     * @param input     input {@link SequenceFile} path
     * @param output    output path (this will contain ONE part with the length)
     * @return zero if successful
     * @throws java.lang.Exception
     */
    public int initJob(JobConf jobConf, String input, String output, boolean cleanLogs) throws Exception {
        JobConf conf = new JobConf(jobConf, TotalSequenceLength.class);
        conf.setJobName("TotalSequenceLength");
        
        // We can only handle one reducer
        if (conf.getNumReduceTasks() != 1) {
            conf.setNumReduceTasks(1);
            LOG.info("Setting number of reducers to ONE!");
        }

        SequenceFileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setMapperClass(SequenceMapClass.class);
        conf.setOutputKeyClass(IntWritable.class);         // map output key class
        conf.setOutputValueClass(IntWritable.class);       // map output value class

        conf.setCombinerClass(LengthReduceClass.class);
        conf.setReducerClass(LengthReduceClass.class);
        FileOutputFormat.setOutputPath(conf, new Path(output));

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
        System.out.println("TotalSequenceLength [-m <maps>] [-r <reduces>]  [-c] <input> <output>");
        System.out.println("\twhere '-c' will clean (remove) the logs when done");
        ToolRunner.printGenericCommandUsage(System.out);
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

        int res = initJob(conf, other_args.get(0), other_args.get(1), cleanLogs);
        int cnt = this.getCount(conf, other_args.get(1));
        System.out.printf("Total length of sequences is %d\n", cnt);
        return res;
    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new TotalSequenceLength(), args);
        System.exit(res);
    }

}

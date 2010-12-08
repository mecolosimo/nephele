/**
 * Created on April 3, 2009.
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

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.mitre.ccv.mapred.io.CompositionVectorKey;
import org.mitre.ccv.mapred.io.CompositionVectorWritable;
import org.mitre.ccv.mapred.io.KmerEntropyPairWritable;

import org.mitre.la.SparseVector;
import org.mitre.la.VectorLengthException;
import org.mitre.la.mapred.io.SparseVectorWritable;
import org.mitre.mapred.fs.FileUtils;

/**
 * Map-Reduce class that generates feature vectors as {@link SparseVectorWritable}s.
 *
 * <P>This can use a given (or serilized) {@link ArrayList} to limit what add to the vector.
 * If <B>NO</B> sample contains a k-mer in the list, it <B>WILL BE</B> included in the final vector.
 *
 * @author Marc Colosimo
 */
public class GenerateFeatureVectors extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(GenerateFeatureVectors.class);
    private static final String KMER_LIST = "ccv.mapred.feature.vector.kmer.listfile";
    private static final String VECTOR_CARDINALITY = "ccv.mapred.feature.vector.cardinality";

    /**
     * Returns a read only {@link MappedByteBuffer}.
     * 
     * @param input full <b>local</b>  path 
     * @throws java.io.IOException
     */
    public static MappedByteBuffer getMappedByteBuffer(String input) throws IOException {
        FileInputStream fins = new FileInputStream(input);
        FileChannel channel = fins.getChannel();
        return channel.map(FileChannel.MapMode.READ_ONLY, 0, (int) channel.size());
    }

    public static class CompositionVectorMap extends MapReduceBase
            implements Mapper<CompositionVectorKey, CompositionVectorWritable, Text, SparseVectorWritable> {

        private FileSystem localFs;                 // local (node) filesystem
        private Text name = new Text();
        private SparseVectorWritable vector = new SparseVectorWritable();
        private MappedByteBuffer listBuffer = null;
        private int cardinality = -1;

        @Override
        public void configure(JobConf conf) {
            this.cardinality = conf.getInt(VECTOR_CARDINALITY, -1);
            try {
                this.localFs = FileSystem.getLocal(new Configuration());
                //localArchives = DistributedCache.getLocalCacheArchives(job);
                String listInput = conf.get(KMER_LIST, null);
                Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

                if (listInput != null && localFiles.length != 0) {
                    //System.err.println("Got list of local cache files!");
                    // only expecting one but we should check
                    //System.err.printf("Looking for %s\n", listInput);
                    for (int cv = 0; cv < localFiles.length; cv++) {
                        // getName()=kmer_1208b0a341b_tmp
                        //System.err.printf("Got cache file %s(%s)\n", localFiles[cv].toString(), localFiles[cv].getName());
                        if (!localFiles[cv].getName().equals(listInput)) {
                            continue;
                        }
                        // /state/partition1/hadoop/var/hadoop-root/mapred/local/taskTracker/archive/rocks5.local/user/mcolosimo/kmer_1208b0a341b_tmp/kmer_1208b0a341b_tmp
                        //System.err.println("Got matching file: " + localFiles[cv].toString());
                        // Full path to cached file to make a read only MappedByteBuffer
                        this.listBuffer = getMappedByteBuffer(localFiles[cv].toString());
                        if (this.listBuffer == null) {
                            LOG.warn("Buffer returned is null!");
                        }
                        // capacity and remaining are the same here, but hasRemaining returns false until we rewind!
                        //System.err.printf("Buffer has %d capacity with %d remaining.\n", this.listBuffer.capacity(), this.listBuffer.remaining());
                        break;
                    }
                }
            // hopefully throws exception when map is called, just log it here
            } catch (IOException ex) {
                LOG.fatal("Unable to get cached files", ex);
            } 
        }

        @Override
        public void close() throws IOException {
            this.localFs.close();
        }

        @Override
        public void map(CompositionVectorKey key, CompositionVectorWritable value,
                OutputCollector<Text, SparseVectorWritable> output,
                Reporter reporter) throws IOException {
            if (this.cardinality <= 0) {
                throw new IOException("Cardinality less than or equal to zero!");
            }
            // 
            if (this.listBuffer == null ) {
                throw new IOException("Local list of k-mers is missing!");
            } else {
                // Reset/rewind/clear if not first time here
                //this.listBuffer.clear();
                this.listBuffer.rewind();     // might use clear()
            }
            if (this.listBuffer.remaining() <= 0) {
                throw new IOException("Local list of k-mers is empty!");
            }
            reporter.setStatus(String.format("%s:%d", key.getName(), key.getWindowSize()));
            int windowSize = key.getWindowSize();
            name.set(key.getName());
            KmerEntropyPairWritable w = new KmerEntropyPairWritable();
            SparseVector sv = new SparseVector(key.getName(), this.cardinality);
        
            int cv = 0;
            while (this.listBuffer.hasRemaining()) {
                w.readFields(this.listBuffer);
                String kmer = w.getKey();
                //System.err.printf("Got '%s' k-mer from list.\n", kmer);
                if (kmer.length() == windowSize) {
                    if (value.containsKey(kmer)) {
                        /**
                        if (value.getValue(kmer).isInfinite()) {
                            //System.err.printf("INFINITE: %s %s(%d):%f\n", key.getName(), kmer, cv, value.getValue(kmer));
                            LOG.warn(String.format("INFINITE: %s %s(%d):%f\n", key.getName(), kmer, cv, value.getValue(kmer)));
                            throw new IOException(String.format("INFINITE: %s %s(%d):%f\n", key.getName(), kmer, cv, value.getValue(kmer)));
                        }
                        */
                        //System.err.printf("%s(%d):%f\n", kmer, cv, value.getValue(kmer));
                        sv.set(cv, value.getValue(kmer));
                    }
                }
                cv++;
            }
            vector.set(sv);
            output.collect(name, vector);
        }
    }

    public static class Features2VectorReducer extends MapReduceBase
            implements Reducer<Text, SparseVectorWritable, Text, SparseVectorWritable> {

        private int cardinality;
        private SparseVectorWritable svw = new SparseVectorWritable();

        @Override
        public void configure(JobConf conf) {
            this.cardinality = conf.getInt(VECTOR_CARDINALITY, -1);
        }

        @Override
        public void reduce(Text key, Iterator<SparseVectorWritable> values,
                OutputCollector<Text, SparseVectorWritable> output,
                Reporter reporter) throws IOException {

            // We should be able to get the cardinality from the values
            if (this.cardinality <= 0 ) {
                throw new IOException("Cardinality less than or equal to zero!");
            }
            SparseVector sv =
                    new SparseVector(key.toString(), this.cardinality);
            SparseVectorWritable w;
            while (values.hasNext()) {
                w = values.next();
                try {
                    sv.plusEquals(w.get());
                } catch (VectorLengthException ex) {
                    LOG.warn(ex);
                }
            }
            svw.set(sv);

            /** DEBUG 
            System.out.printf("%s=", key.toString());
            for(Map.Entry<Integer, Double> entry : sv.getSparseMap().entrySet()) {
                System.out.printf(" %d:%f", entry.getKey(), entry.getValue());
            }
            System.out.printf("\n");
             /* */
            output.collect(key, svw);
        }
    }

    /**
     * Start a new job with the given configuration and parameters.
     *
     * @param jobConf
     * @param listInput         file path containing list of k-mers to use
     * @param cardinality       number of k-mers to use (if list contains less,then that will be used instead).
     * @param input             composition vector {@link SequenceFile} such as generated by {@link CalculateCompositionVectors}
     * @param output
     * @param cleanLogs
     * @return zero if no errors
     * @throws java.lang.Exception
     */
    public int initJob(JobConf jobConf, String listInput, Integer cardinality, String input, String output, boolean cleanLogs) throws Exception {
        JobConf conf = new JobConf(jobConf, GenerateFeatureVectors.class);
        conf.setJobName("GenerateFeatureVectors");

        Path listPath = new Path(listInput);  // i.e, listInput = win32_200902260829/kmer_120811a7fa1_tmp
        FileSystem fs = listPath.getFileSystem(conf);
        if (listInput != null) {
            // @todo: should check to see if it is there!
            
            // It doesn't say it, but we need the quailifed path with the host name
            // otherwise URI sticks the host on to it not so nicely
            Path qPath = fs.makeQualified(listPath);
            // listPath = hdfs://rocks5.local:54310/user/mcolosimo/win32_200902260829/kmer_120811a7fa1_tmp
            LOG.info(String.format("Caching k-mer file %s", qPath.toString()));
            // URI:hdfs://rocks5.local:54310/user/mcolosimo/win32_200902260829/kmer_120811a7fa1_tmp
            URI listURI = new URI(qPath.toString());
            DistributedCache.addCacheFile(listURI, conf);
            conf.set(KMER_LIST, listPath.getName());
            //LOG.info("k-mer URI:" + listURI.toString());
        } else {
            throw new Exception("GenerateFeatureVectors requires a list of k-mers!");
        }

        /** We need this. It is okay if the cardinality is larger than the number of k-mers. */
        if (cardinality == null) {
            LOG.info("Scanning k-mer file to determine cardinality");
            FSDataInputStream ins = fs.open(listPath);

            KmerEntropyPairWritable w = new KmerEntropyPairWritable();
            int c = 0;
            while(ins.available() > 0) {
                w.readFields(ins);
                c++;
            }
            ins.close();
            fs.close();
            LOG.info(String.format("Found %d k-mers in the file", c));
            cardinality = c;
        }
        conf.setInt(VECTOR_CARDINALITY, cardinality);

        // Set up mapper
        SequenceFileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setMapperClass(CompositionVectorMap.class);
        conf.setOutputKeyClass(Text.class);                    // final output key class - sample name
        conf.setOutputValueClass(SparseVectorWritable.class);  // final output value class

        // Set up combiner/reducer
        conf.setReducerClass(Features2VectorReducer.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);

        return 0;
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf());
        int cardinality = Integer.MAX_VALUE;
        boolean cleanLogs = false;
        String listInput = null;

        // @TODO: use commons getopts, org.apache.hadoop.util.GenericOptionsParser used it
        ArrayList<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else if ("-c".equals(args[i])) {
                    cleanLogs = true;
                } else if ("-l".equals(args[i])) {
                    listInput = args[++i];
                } else if ("-t".equals(args[i])) {
                    cardinality = Integer.parseInt(args[++i]);
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
                    other_args.size() + " instead of 3.");
            return printUsage();
        }

        if (listInput == null || listInput.length() == 0) {
            System.out.println("Need kmer sequence file path!");
            return printUsage();
        }

        long now = System.currentTimeMillis();
        Path listInputPath = new Path(listInput);
        Path listOutputPath = new Path(listInputPath.getParent(),"kmer_" + Long.toHexString(now) + "_tmp");
        LOG.info(String.format("Loading %d sorted k-mers from %s to %s", cardinality, listInputPath.toString(), listOutputPath.toString()));
        int num = CompleteCompositionVectorUtils.flattenKmerEntropySequenceFile(conf, cardinality, listInputPath.toString(), listOutputPath.toString(), cleanLogs);

        initJob(conf, listOutputPath.toString(), num, other_args.get(0), other_args.get(1), cleanLogs);
        return 0;
    }

    static int printUsage() {
        System.out.println("GenerateFeatureVectors [-libjars <classpath,...>] [-m <maps>] [-r <reduces>] [-t <num kmers>] -l <kmer list path>" +
                " <input> <output>");
        System.out.println("Where -l give the path to the SequenceFiles containing the k-mers");
        return -1;
    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new GenerateFeatureVectors(), args);
        System.exit(res);
    }
}

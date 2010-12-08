/**
 * Created on October 12, 2009.
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
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
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
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.mitre.la.DenseVector;
import org.mitre.la.SparseVector;
import org.mitre.la.VectorLengthException;
import org.mitre.la.mapred.io.DenseVectorWritable;
import org.mitre.la.mapred.io.SparseVectorWritable;

import org.mitre.mapred.fs.FileUtils;
import org.mitre.mapred.io.StringDoublePairWritable;

/**
 * MapReduce class for calculating a distance matrix from feature vectors.
 *
 * @author Marc Colosimo
 */
public class CalculateCosineDistanceMatrix extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(CalculateCosineDistanceMatrix.class);
    public static final String CACHE_PATH = "ccv.distance.labels.path";
    public static final String DISTANCE_MATRIX_SIZE = "ccv.distance.matrix.size";

    /**
     * Pre-computes the norm2 of each vector for the cached matrix
     */
    public static class Norm2Mapper extends MapReduceBase
            implements Mapper<Text, SparseVectorWritable, StringDoublePairWritable, SparseVectorWritable> {

        private final StringDoublePairWritable norm2 = new StringDoublePairWritable();

        @Override
        public void map(Text key, SparseVectorWritable value,
                OutputCollector<StringDoublePairWritable, SparseVectorWritable> output, Reporter reporter) throws IOException {

            reporter.setStatus(String.format("Calculating norm2 for %s", key.toString()));
            norm2.set(key.toString(), value.get().calculateNorm2());
            output.collect(norm2, value);
        }
    }

    /**
     * Generate the output for the lower triangle by reading in a <b>sorted</b> list of labels.
     */
    public static class DistanceMap extends MapReduceBase
            implements Mapper<Text, SparseVectorWritable, Text, SparseVectorWritable> {

        private FileSystem localFs;                 // local (node) filesystem
        private final Text mPosition = new Text();
        private final Text nPosition = new Text();
        private  Path cachedVectors = null;
        private JobConf conf;
        private int size;                   // final size of matrix

        @Override
        public void configure(JobConf conf) {
            this.size = conf.getInt(DISTANCE_MATRIX_SIZE, -1);
            try {
                this.conf = conf;
                this.localFs = FileSystem.getLocal(new Configuration());
                //localArchives = DistributedCache.getLocalCacheArchives(job);
                String cacheInput = conf.get(CACHE_PATH, null);
                Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

                if (cacheInput != null && localFiles.length != 0) {
                    for (int cv = 0; cv < localFiles.length; cv++) {
                        if (!localFiles[cv].getName().equals(cacheInput)) {
                            continue;
                        }
                        //System.err.printf("cached file name is %s\n", localFiles[cv].toString());
                        LOG.info(String.format("Cached SequenceFile is at %s", localFiles[cv].toString()));
                        // Why does hadoop repeat the file name in a directory? This is part-00000/part-00000
                        this.cachedVectors = localFiles[cv];
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

        public String readTextBytes(ByteBuffer in) throws IOException {
            int length = in.getInt();
            byte[] bytes = new byte[length];
            in.get(bytes, 0, length);
            return Text.decode(bytes);
        }

        @Override
        public void map(Text key, SparseVectorWritable value,
                OutputCollector<Text, SparseVectorWritable> output,
                Reporter reporter) throws IOException {

            if (this.cachedVectors == null) {
                throw new IOException("Cached vectors is missing!");
            }
            LOG.info(String.format("Reading vectors from %s", this.cachedVectors.toString()));
            final SequenceFile.Reader reader = new SequenceFile.Reader(this.localFs, this.cachedVectors, this.conf);
            reporter.setStatus(String.format("Working on vector %s", key.toString()));
            //System.out.printf("Working on vector %s\n", key.toString());
            //System.out.printf("\t%s\n", value.get().vector2String(0, 6));

            final String firstLabel = key.toString();
            final SparseVector valueVector = value.get();
            final double valueNorm2 = valueVector.calculateNorm2();
            final StringDoublePairWritable cachedKey = new StringDoublePairWritable();
            final SparseVectorWritable cachedValue = new SparseVectorWritable();
            final SparseVector row = new SparseVector(key.toString(),this.size);       //will error if -1
            final SparseVectorWritable vectorWritable = new SparseVectorWritable();
            mPosition.set(firstLabel);
            boolean upperTriangle = false;
            int keyIndex = -1;      // index of our key label
            int index = 0;
            try {
                boolean hasNext = reader.next(cachedKey, cachedValue);
                while (hasNext) {
                    final String secondLabel = cachedKey.getKmer();
                    final SparseVector cachedVector = cachedValue.get();
                    final SparseVector column = new SparseVector(" ",this.size); // we don't use this label so make it empty
                    if (!upperTriangle && secondLabel.equals(firstLabel)) {
                        upperTriangle = true;
                    }
                    if (firstLabel.equals(secondLabel)) {
                        row.set(index, 0.0);
                        keyIndex = index;
                    } else {
                        nPosition.set(secondLabel);
                        if (upperTriangle) {
                            double dot = valueVector.dot(cachedVector) /
                                    (valueNorm2 * cachedKey.getValue());
                            dot = (1.0 - dot) / 2.0;
                            if (dot < 0) {
                                dot = 0.0;
                            }
                            column.set(keyIndex, dot);
                            vectorWritable.set(column);
                            output.collect(nPosition, vectorWritable);      // lower triangle value
                            row.set(index, dot);                            // upper triangle value
                            //System.out.printf("for %s, wrote out %f for %s,%s\n", firstLabel, dot, firstLabel, secondLabel);
                        } 
                    }

                    hasNext = reader.next(cachedKey, cachedValue);
                    index += 1;
                    //System.err.printf("Wrote out %s and %s combos\n", key.toString(), secondLabel);
                }
                vectorWritable.set(row);
                output.collect(mPosition, vectorWritable);
            } catch (VectorLengthException vle) {
                throw new IOException(vle);
            }

            try {
                reader.close();
            } catch (IOException ioe) {
                LOG.debug("Exception closing SequenceFile.Reader!");
            }
        }
    }
    
    /**
     * Merges (Reduces) the lower triangle distances into row based dense vectors.
     */
    public static class DistanceReducer extends MapReduceBase
            implements Reducer<Text, SparseVectorWritable, Text, DenseVectorWritable> {

        private int size;
        final private DenseVectorWritable vectorWritable = new DenseVectorWritable();

        @Override
        public void configure(JobConf conf) {
            this.size = conf.getInt(DISTANCE_MATRIX_SIZE, -1);
        }

        @Override
        public void reduce(Text key, Iterator<SparseVectorWritable> values, OutputCollector<Text, DenseVectorWritable> output,
                Reporter reporter) throws IOException {

            if (this.size <= 0) {
                throw new IOException(String.format("Matrix size (%d) is not set correctly", this.size));
            }
            final DenseVector denseVector = new DenseVector(key.toString(), this.size);

            reporter.setStatus(String.format("Merging row %s into a vector", key.toString()));
            //System.out.printf("Merging row %s into a vector\n", key.toString());

            while (values.hasNext()) {
                final SparseVectorWritable value = values.next();
                for (Map.Entry<Integer, Double> entry:value.get().getSparseMap().entrySet()) {
                    denseVector.set(entry.getKey(), entry.getValue());
                }
            }
            this.vectorWritable.set(denseVector);
            output.collect(key, this.vectorWritable);
        }
    }

    public void writeTextBytes(FSDataOutputStream fos, Text text) throws IOException {
        byte[] bytes = text.getBytes();
        int length = bytes.length;
        fos.writeInt(length);
        fos.write(bytes, 0, length);
    }

    /**
     * Writes out the matrix in row major (packed) order. No labels are outputed.
     *
     * @param jobConf
     * @param input
     * @param output
     * @param digits
     * @throws IOException
     */
    public static void printRowMajorMatrix(JobConf jobConf, String input, String output, int digits) throws IOException {
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
        final DenseVectorWritable value = new DenseVectorWritable();
        for (int idx = 0; idx < paths.length; idx++) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[idx], conf);
            boolean hasNext = reader.next(key, value);
            while (hasNext) {

                final DenseVector vector = value.get();
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
     * Outputs the distance matrix (DenseVectors) in Phylip Square format. Names/labels are limited to 10-characters!
     *
     * @param jobConf
     * @param input             input directory name containing DenseVectors (as generated by this class).
     * @param output            output file name
     * @param fractionDigits    number of digits after decimal point
     * @throws IOException
     */
    public static void printPhylipSquare(JobConf jobConf, String input, String output, int fractionDigits) throws IOException {
        JobConf conf = new JobConf(jobConf, CalculateCosineDistanceMatrix.class);

        DecimalFormat format = new DecimalFormat();
        format.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.US));
        format.setMinimumIntegerDigits(1);
        format.setMaximumFractionDigits(fractionDigits);
        //format.setMinimumFractionDigits(fractionDigits);
        format.setGroupingUsed(false);

        final Path inputPath = new Path(input);
        final FileSystem fs = inputPath.getFileSystem(conf);
        final Path qInputPath = fs.makeQualified(inputPath);
        final Path outputPath = new Path(output);
        Path[] paths = FileUtils.ls(conf, qInputPath.toString() + Path.SEPARATOR + "part-*");

        FSDataOutputStream fos = fs.create(outputPath, true);   // throws nothing!
        Writer writer = new OutputStreamWriter(fos);
        Text key = new Text();
        DenseVectorWritable value = new DenseVectorWritable();
        Boolean wroteHeader = false;
        for (int idx = 0; idx < paths.length; idx++) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, paths[idx], conf);
            boolean hasNext = reader.next(key, value);
            while (hasNext) {
                
                final DenseVector vector = value.get();
                if (!wroteHeader) {
                    writer.write(String.format("\t%d\n", vector.getCardinality()));
                    wroteHeader = true;
                }

                final StringBuilder sb = new StringBuilder();
                final String name = key.toString();
                sb.append(name.substring(0, (name.length() > 10 ? 10 : name.length())));
                final int padding = Math.max(1, 10 - name.length());
                for (int k = 0; k < padding; k++) {
                    sb.append(' ');
                }
                sb.append(' ');
                for (int i = 0; i < vector.getCardinality(); i++) {
                    final String s = format.format(vector.get(i)); // format the number
                    sb.append(s);
                    sb.append(' ');
                }
                sb.append("\n");
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

    public int initJob(JobConf jobConf, String input, String output) throws Exception {
        JobConf conf = new JobConf(jobConf, CalculateCosineDistanceMatrix.class);

        final Path inputPath = new Path(input);
        final FileSystem fs = inputPath.getFileSystem(conf);
        final Path qInputPath = fs.makeQualified(inputPath);

        /**
         * Need to get all of the sample names/labels
         */
        JobConf cacheConf = new JobConf(jobConf, CalculateCosineDistanceMatrix.class);
        cacheConf.setJobName("CacheNorm2MapReduce");
        cacheConf.setNumReduceTasks(1);         // Want ONE part file

        // Set up IdentityMapper
        SequenceFileInputFormat.setInputPaths(cacheConf, new Path(input));
        cacheConf.setInputFormat(SequenceFileInputFormat.class);
        cacheConf.setMapperClass(Norm2Mapper.class);
        cacheConf.setOutputKeyClass(StringDoublePairWritable.class);
        cacheConf.setOutputValueClass(SparseVectorWritable.class);

        // Set up IdentityReducer
        cacheConf.setReducerClass(IdentityReducer.class);
        cacheConf.setOutputFormat(SequenceFileOutputFormat.class);
        cacheConf.setNumReduceTasks(1);
        Path sfPath = FileUtils.createRemoteTempPath(fs, qInputPath.getParent());
        LOG.info(String.format("Generating feature vector SequenceFile path %s", sfPath.toString()));
        SequenceFileOutputFormat.setOutputPath(cacheConf, sfPath);
        JobClient.runJob(cacheConf);

        Path cachePath = new Path(sfPath.toString() + Path.SEPARATOR + "part-00000");

        // need to know the size (the reducer might be able to send this back via the Reporter, but how do we grab that info?
        StringDoublePairWritable key = new StringDoublePairWritable();
        int size = 0;
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, cachePath, conf);
        boolean hasNext = reader.next(key);
        while (hasNext) {
            size += 1;
            hasNext = reader.next(key);
        }
        try {
            reader.close();
        } catch (IOException ioe) {
            // closing the SequenceFile.Reader will throw an exception if the file is over some unknown size
            LOG.debug("Probably caused by closing the SequenceFile.Reader. All is well", ioe);
        }

        //LOG.info(String.format("Caching model file %s", qInputPath.toString()));
        URI listURI = new URI(fs.makeQualified(cachePath).toString());
        DistributedCache.addCacheFile(listURI, conf);
        LOG.info(String.format("SequenceFile cache path %s (%s) with %d labels", listURI.toString(), cachePath.getName(), size));
        conf.set(CACHE_PATH, cachePath.getName());
        conf.setInt(DISTANCE_MATRIX_SIZE, size);

        /**
         * Main MapReduce Task of generating dot products
         */
        LOG.info("Generating distances");
        JobConf distanceConf = new JobConf(conf, CalculateCosineDistanceMatrix.class);
        distanceConf.setJobName("DistanceMapReduce");
        // Set up distance mapper
        SequenceFileInputFormat.setInputPaths(distanceConf, new Path(input));
        distanceConf.setInputFormat(SequenceFileInputFormat.class);
        distanceConf.setMapperClass(DistanceMap.class);
        distanceConf.setMapOutputKeyClass(Text.class);
        distanceConf.setMapOutputValueClass(SparseVectorWritable.class);

        // Set up reducer to merge lower-triangle results into a single dense distance vector
        distanceConf.setReducerClass(DistanceReducer.class);
        distanceConf.setOutputKeyClass(Text.class);
        distanceConf.setOutputValueClass(DenseVectorWritable.class);
        distanceConf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(distanceConf, new Path(output));
        JobClient.runJob(distanceConf);

        return 0;
    }

    static int printUsage() {
        System.out.println("CalculateCosineDistanceMatrix [-m <maps>] [-r <reduces>] [-D <property=value>] [-packedRow <file>] [-digits <number>] [-phylip <file>] <input> <output>");
        System.out.println("\tWhen using phylip or packedRow, <outfile> is not required and no mapreduce jobs will be run");
        System.out.println("\tdigits specify the number of fractions digits (numbers after the period). Default is 6 for writing out");
        return -1;
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf());

        String phylip = null;
        String packedRow = null;
        int fractionDigits = 6;

        //String userJarLocation = "/path/to/jar";
        //conf.setJar(userJarLocation); //were conf is the JobConf object
        ArrayList<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else if ("-D".equals(args[i])) {
                    String[] props = args[++i].split("=");
                    conf.set(props[0], props[1]);
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
                } else if ("-phylip".equals(args[i])) {
                    phylip = args[++i];
                } else if ("-packedRow".equals(args[i])) {
                    packedRow = args[++i];
                }else if ("-digits".equals(args[i])) {
                    fractionDigits = Integer.parseInt(args[++i]);
                }else {
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

        boolean writeMatrix = (phylip != null || packedRow != null) ? true:false;

        // Make sure there are exactly 3 parameters left.
        if ( (other_args.size() != 2 && !writeMatrix) || (other_args.size() == 0 && writeMatrix)) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");

            return printUsage();
        }

        int ret = 0;
        if (other_args.size() == 2) {
            ret = this.initJob(conf, other_args.get(0), other_args.get(1));
        }
        // check writing out in Phylip format
        if (ret == 0 && other_args.size() == 1 && phylip != null) {
            printPhylipSquare(conf, other_args.get(0), phylip, fractionDigits);
        } else if (ret == 0 && other_args.size() == 2 && phylip != null) {
            printPhylipSquare(conf, other_args.get(1), phylip, fractionDigits);
        }

        // check writing out in row packed order
        if (ret == 0 && other_args.size() == 1 && packedRow != null) {
            printRowMajorMatrix(conf, other_args.get(0), packedRow, fractionDigits);
        } else if (ret == 0 && other_args.size() == 2 && packedRow != null) {
            printRowMajorMatrix(conf, other_args.get(1), packedRow, fractionDigits);
        }

        return ret;
    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new CalculateCosineDistanceMatrix(), args);
        System.exit(res);
    }
}

/**
 * Created on APril 13, 2009.
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
package org.mitre.ccv.weka.mapred;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.mitre.ccv.index.IndexedCompleteCompositionVector;
import org.mitre.ccv.index.IndexedCompositionDistribution;
import org.mitre.mapred.io.StringDoublePairWritable;
import org.mitre.ccv.weka.AbstractWekaCompleteCompositionVector;
import org.mitre.ccv.weka.AbstractWekaCompleteCompositionVector.LabeledInstance;

import org.mitre.mapred.fs.FileUtils;

import weka.core.Attribute;
import weka.core.Instance;

/**
 * A Map-Reduce class that uses a Complete Composition Vector Weka classifiers 
 * {@link org.mitre.ccv.weka.AbstractWekaCompleteCompositionVector}, J48 decision tree
 * {@link org.mitre.ccv.weka.CompleteCompositionVectorJ48} and an SMO SVM
 * {@link org.mitre.ccv.weka.CompleteCompositionVectorSMO} to
 * classify complete composition vectors generated from sequeces.
 * <P>
 * This can only classify (i.e., doesn't do the learning). A pre-built trained model is required.
 * Also, the confidence measurements from the SMO seem to be very low (not very meaningful).
 * <P>
 * This currently is a memory hog and can trigger {@link OutOfMemoryError}
 * exceptions (many of which are not from this class).
 *
 * <p>Some properties (<code>-D property=value</code>) supported are:
 * <ul>
 * <li>weka.output.sortby which determines the sort order of the output.</li>
 * </ul>
 * 
 * @see {@link org.mitre.ccv.weka.CompleteCompositionVectorJ48}
 * @author Marc Colosimo
 */
public class ClassifyInstances extends Configured implements Tool {

    /**
     * This uncovers several bugs and limitations of hadoop and JDK6
     * - lots of OutOfMemoryExceptions all over the place because this is highly threaded
     *
     * GC issues caused by this mapping, since we create a new DistributionIndex each time.
     *
     * Several options exist such as
     *  - Use parallel collector (also known as the throughput collector) with "-XX:+UseParallelGC"
     *    and (optionally) enable parallel compaction with "-XX:+UseParallelOldGC".
     *  - Use concurrent collector with "-XX:+UseConcMarkSweepGC"
     *    and (optionally) enable incremental mode with "-XX:+CMSIncrementalMode"
     *  - play with the heap options
     *
     * See <http://java.sun.com/javase/technologies/hotspot/gc/gc_tuning_6.html> for massive list of options.
     *
     * See <http://hadoop.apache.org/core/docs/current/mapred_tutorial.html#Task+Execution+%26+Environment>
     * mapred.child.java.opts
     *
     * Also if we run this with many maps on the same machine (-m 50 out of 56 on 13 mixed machines)
     * we run out of memory and expose this bug
     * <http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6521677>
     * 
     */
    private static final Log LOG = LogFactory.getLog(ClassifyInstances.class);
    public static final String MODEL_PATH = "weka.classifier.model.path";

    /** Property for setting classifier class name. */
    public static final String CLASSIFIER = "weka.classifier.class";

    /** Default classifier class name. */
    public static final String DEFAULT_CLASSIFIER = "org.mitre.ccv.weka.CompleteCompositionVectorJ48";

    /** Property name for how to format (tab-delimited) and sort the output. */
    public static final String SORT_OUTPUT_BY = "weka.output.sortby";
    /** Sort the output by the sample name (default). Format: sample name, class name, confidence. */
    public static final String SORTBY_SAMPLE = "sample";
    /** Sort the output by the class instance (name). Format: class name, confidence, sample name. */
    public static final String SORTBY_CLASS = "class";
    /** Sort the output by the classifiers confidence (highest to lowest). Format: class name, confidence, sample name. */
    public static final String SORTBY_CONFIDENCE = "confidence";
    /** Sort the output by the class instance then confidence in that class. Format: class name, confidence, sample name. */
    public static final String SORTBY_CLASS_CONFIDENCE = "class.confidence";

    public static class CompositionVectorJ48Map extends MapReduceBase
            implements Mapper<Text, Text, WritableComparable, Text> {

        private static final Log MAP_LOG = LogFactory.getLog(CompositionVectorJ48Map.class);
        private AbstractWekaCompleteCompositionVector classifier = null;
        private FileSystem localFs = null;
        private Attribute classAttribute = null;
        //private Text classText = new Text();
        private String sortBy = null;
        private Text outText = new Text();
        //private Text outSampleName = new Text();
        private StringDoublePairWritable outClassConfidence = new StringDoublePairWritable();

        @SuppressWarnings("unchecked")
        public <T> T newClassifierInstance(Class<T> theClassifier, String inputFile) {
            T result;
            try {
                Constructor<T> meth = theClassifier.getDeclaredConstructor(new Class[]{String.class});
                //meth.setAccessible(true);
                result = meth.newInstance(inputFile);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return result;
        }

        @Override
        public void configure(JobConf conf) {
            sortBy = conf.get(SORT_OUTPUT_BY, null);
            try {
                this.localFs = FileSystem.getLocal(new Configuration());
                String listInput = conf.get(MODEL_PATH, null);
                Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

                if (listInput != null && localFiles.length != 0) {

                    for (int cv = 0; cv < localFiles.length; cv++) {
                        if (!localFiles[cv].getName().equals(listInput)) {
                            continue;
                        }
                        Class<? extends AbstractWekaCompleteCompositionVector> classifierClass =
                           (Class<? extends AbstractWekaCompleteCompositionVector>) 
                           conf.getClassByName(conf.get(CLASSIFIER, DEFAULT_CLASSIFIER));

                        // We might choke loading the model into memory!
                        // It would be nice to use nio classes especially MappedByteBuffer
                        //AbstractWekaCompleteCompositionVector classifierClass =
                        //        conf.getClass(CLASSIFIER, CompleteCompositionVectorJ48.class);
                        this.classifier = newClassifierInstance(classifierClass, localFiles[cv].toString());
                        //Constructor<CompleteCompositionVectorJ48> meth =
                        //        CompleteCompositionVectorJ48.class.getDeclaredConstructor(new Class[]{String.class});

                        //this.classifier = meth.newInstance(localFiles[cv].toString());

                        //this.classifier = ReflectionUtils.newInstance(CompleteCompositionVectorJ48.class, conf);
                        //this.classifier = new CompleteCompositionVectorJ48(localFiles[cv].toString());
                        this.classAttribute = (Attribute) this.classifier.getAttributes().elementAt(this.classifier.getClassIndex());
                        break;
                    }
                }
            // hopefully throws exception when map is called, just log it here
            } catch (FileNotFoundException ex) {
                MAP_LOG.fatal("Unable to get cached file", ex);
            //  } catch (ClassNotFoundException ex) {
            //      MAP_LOG.fatal("Unable to get cached file", ex);
            } catch (IOException ex) {
                MAP_LOG.fatal("Unable to get cached file", ex);
            } catch (ClassNotFoundException ex) {
                 throw new RuntimeException(ex);
            }
            if (this.classifier == null) {
                MAP_LOG.warn("Classifier was not loaded!");
            }
        }

        public void formatOuput(OutputCollector<WritableComparable, Text> output,
                Text sampleName, String clsValue, Double clsDist) throws IOException {

            this.outClassConfidence.set(clsValue, clsDist);
            //this.outSampleName.set(sampleName);
            if (this.sortBy != null && !this.sortBy.equals(SORTBY_SAMPLE)) {
                if (this.sortBy.equals(SORTBY_CONFIDENCE)) {
                    // reverse sort order
                    this.outClassConfidence.compareValues(true);
                } else if (this.sortBy.equals(SORTBY_CLASS_CONFIDENCE)) {
                    // keep natural order for the key, reverse for values
                    this.outClassConfidence.compareKeyValues(false, true);
                } // SORTBY_CLASS (key) is default
                output.collect(this.outClassConfidence, sampleName);
            } else {

                this.outText.set(String.format("%s\t%f", clsValue, clsDist));
                output.collect(sampleName, this.outText);
            }
        }

        @Override
        public void map(Text key, Text value, OutputCollector<WritableComparable, Text> output,
                Reporter reporter) throws IOException {

            if (this.classifier == null) {
                throw new IOException("No classifier!");
            }
            if (this.classAttribute == null) {
                throw new IOException("Have classifier, but no classes!");
            }
            int start = this.classifier.getBegin();
            int end = this.classifier.getEnd();

            /**
             * IndexedCompositionDistribution will throw IllegalArgumentException
             */
            String seq = value.toString();
            if (end > seq.length()) {
                LOG.info(String.format("%s length (%d) is smaller than the end window size (%d). Skipping..",
                        key.toString(), seq.length(), end));
                return;
            }
            // DEBUG
            if (seq.length() > 1000000) {
                LOG.warn(String.format("Skipping %s because it is to long (%d)", key.toString(), seq.length()));
                System.out.printf("Skipping %s because it is to long (%d)", key.toString(), seq.length());
                return;
            }

            /**
             * Generate the complete composition vector for this sample.
             * IndexedCompositionDistribution might throw IllegalArgumentException.
             */
            reporter.setStatus(String.format("Classifying %s using window sizes of %d to %d",
                    key.toString(), start, end));
            IndexedCompleteCompositionVector ccv;
            try {
                IndexedCompositionDistribution cd =
                        new IndexedCompositionDistribution(null, 1, seq, start, end);
                ccv =
                        new IndexedCompleteCompositionVector(key.toString(), 1,
                        start, end, cd);
            } catch (OutOfMemoryError me) {
                System.gc();
                LOG.warn(String.format(
                        "Ran out of memory while generating complete composition vector for %s of %d length", key.toString(), seq.length()), me);
                System.err.printf("Ran out of memory while generating complete composition vector for %s of %d length", key.toString(), seq.length());

                output.collect(new Text("[ERROR: Out of Memory generating CCV]"), key);
                return;
            }

            /** Generate an instance for this sample. */
            try {
                /** This can throw an exception for something */
                Instance inst = classifier.getInstanceSparse(ccv);
                /** To classify, we need to be part of a DataSet/Instance */
                LabeledInstance li = classifier.runClassifier(inst, classAttribute);

                int clsValue = (int) li.inst.classValue();
                //System.out.printf("%s\t%s\t%f\n", key.toString(),
                //        classAttribute.value(clsValue), li.clsDist[clsValue]);
                //this.classText.set(String.format("%s\t%f", classAttribute.value(clsValue), li.clsDist[clsValue]));
                //output.collect(key, this.classText);
                this.formatOuput(output, key, classAttribute.value(clsValue), li.clsDist[clsValue]);
            } catch (OutOfMemoryError me) {
                System.gc();
                LOG.warn(String.format(
                        "Ran out of memory while classifying vector for %s of %d length", key.toString(), seq.length()), me);
                System.err.printf("Ran out of memory while classifying vector for %s of %d length", key.toString(), seq.length());

            } catch (Exception ex) {
                MAP_LOG.warn("Exception when classifing!", ex);
            }
        }
    }

    public int initJob(JobConf jobConf, String modelInput, String input, String output) throws Exception {
        JobConf conf = new JobConf(jobConf, ClassifyInstances.class);
        conf.setJobName("ClassifyInstances");

        Path listPath = new Path(modelInput);
        FileSystem fs = listPath.getFileSystem(conf);
        if (modelInput != null) {
            Path qPath = fs.makeQualified(listPath);
            LOG.info(String.format("Caching model file %s", qPath.toString()));
            URI listURI = new URI(qPath.toString());
            DistributedCache.addCacheFile(listURI, conf);
            conf.set(MODEL_PATH, listPath.getName());
        } else {
            throw new Exception("ClassifyInstances requires a model!");
        }

        // Set up mapper
        SequenceFileInputFormat.setInputPaths(conf, new Path(input));
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setMapperClass(CompositionVectorJ48Map.class);
        // Painful way to set job output key class because we can't use WritableComparable
        String sortBy = conf.get(SORT_OUTPUT_BY, null);
        if (sortBy != null && !sortBy.equals(SORTBY_SAMPLE)) {
            LOG.info("Sorting output by class name and/or confidence.");
            conf.setOutputKeyClass(StringDoublePairWritable.class);
        } else {
            LOG.info("Sorting output by sample name.");
            conf.setOutputKeyClass(Text.class);
        }
        conf.setOutputValueClass(Text.class);  // job output value class

        // Uses default reducer (IdentityReducer) and save it to a plain text file
        conf.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        return 0;
    }

    static int printUsage() {
        System.out.println("ClassifyInstances [-m <maps>] [-r <reduces>] [-D <property=value>] [-C <classifier class name>]<model> <input> <output>");
        System.out.println("Sorting of output is controlled by the 'weka.output.sortby' property");
        System.out.println("Known classifiers are:\n\torg.mitre.ccv.weka.CompleteCompositionVectorJ48 (default)\n\torg.mitre.ccv.weka.CompleteCompositionVectorSMO\n" +
                "Models must be generated by the same classifier used here.");
        return -1;
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
                } else if ("-C".equals(args[i]) ) {
                    conf.set(CLASSIFIER, args[++i]);
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
        // Make sure there are exactly 3 parameters left.
        if (other_args.size() != 3) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 3.");

            return printUsage();
        }

        return initJob(conf, other_args.get(0), other_args.get(1), other_args.get(2));
    }

    static public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new ClassifyInstances(), args);
        System.exit(res);
    }
}

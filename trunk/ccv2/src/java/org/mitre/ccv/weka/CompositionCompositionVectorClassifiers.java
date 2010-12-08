/**
 * Created on May 19, 2009.
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
package org.mitre.ccv.weka;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.lang.reflect.Constructor;

import java.util.MissingResourceException;
import java.util.NoSuchElementException;
import java.util.ResourceBundle;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import org.mitre.bio.Sequence;
import org.mitre.bio.io.FastaIterator;
import org.mitre.ccv.CompleteCompositionVector;
import org.mitre.ccv.CompleteCompositionVectorMain;
import org.mitre.ccv.index.DistributionIndex;
import org.mitre.ccv.index.IndexedCompleteCompositionVector;
import org.mitre.ccv.index.IndexedCompositionDistribution;
import org.mitre.ccv.weka.AbstractWekaCompleteCompositionVector.LabeledInstance;

import weka.core.Attribute;
import weka.core.Instance;

/**
 * The class supports classifying samples (using classes that extend {@link AbstractWekaCompleteCompositionVector})..
 *
 * <p>The supports cross-validation and saving of the model.
 * 
 * <p> The classifier used can be set on the command-line or system properties:
 * <pre>
 * java -cp ccv.jar org.mitre.ccv.weka.CompositionCompositionVectorClassifiers -D weka.classifier.class=org.mitre.ccv.weka.CompleteCompositionVectorSMO
 * </pre>
 *
 * <P>The default classifier is <code>org.mitre.ccv.weka.CompleteCompositionVectorJ48</code>. Any class that extend
 * {@link AbstractWekaCompleteCompositionVector} are supported.
 *
 * @author Marc Colosimo
 */
public class CompositionCompositionVectorClassifiers {

    private static final Log LOG;
    private static ClassLoader classLoader;
    /**
     * Set-up our static strings
     */
    private static String APPLICATION_TITLE = "CompleteCompositionVectorClassifier";
    private static String APPLICATION_VERSION = "Unknown";
    private static String APPLICATION_BUILD = "Unknown";
    /** Our classifier class */
    private Class<? extends AbstractWekaCompleteCompositionVector> classifierClass = null;
    /** Our classifer instance */
    private AbstractWekaCompleteCompositionVector classifier = null;
    /** The class attribute used loadClassifierFromModel */
    //private Attribute classAttribute = null;
    /** Property (-D) for setting classifier class name. */
    public static final String CLASSIFIER = "weka.classifier.class";
    /** Default classifier class */
    private static final Class<? extends AbstractWekaCompleteCompositionVector> DEFAULT_CLASSIFIER = org.mitre.ccv.weka.CompleteCompositionVectorJ48.class;


    static {
        LOG = LogFactory.getLog(CompositionCompositionVectorClassifiers.class);

        classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = CompositionCompositionVectorClassifiers.class.getClassLoader();
        }

        ResourceBundle bundle = null;
        try {
            bundle = ResourceBundle.getBundle("org.mitre.ccv.ApplicationInformation");
        } catch (MissingResourceException mre) {
            bundle = null;
            LOG.warn("Unable to find ApplicationInformation Bundle");
        }
        if (bundle != null) {
            APPLICATION_VERSION = bundle.getString("application.version");
            APPLICATION_TITLE = APPLICATION_TITLE + " " + APPLICATION_VERSION;

            APPLICATION_BUILD = bundle.getString("subversion.version");
        }
    }

    // move these into org.mitre.util.ReflectionUtils ?
    /**
     * Get the value of the <code>name</code> system property as a <code>Class</code>.
     * If no such property is specified, then <code>defaultValue</code> is
     * returned.
     *
     * @param name the class name.
     * @param defaultValue default value.
     * @return property value as a <code>Class</code>,
     *         or <code>defaultValue</code>.
     */
    @SuppressWarnings("unchecked")
    private Class<?> getClass(String name, Class<?> defaultValue) {
        String valueString = System.getProperty(name);
        if (valueString == null) {
            LOG.debug(String.format("No System property for '%s', so using default class '%s'",
                    name, defaultValue.getName()));
            return defaultValue;
        }
        try {
            return Class.forName(valueString, true, classLoader);
        } catch (ClassNotFoundException e) {
            LOG.fatal("Unable to find class '" + valueString + "'");
            throw new RuntimeException(e);
        }
    }

    /** 
     * Returns a classifier loaded from the given saved model.
     * @param <T>
     * @param theClassifier
     * @param inputFile
     */
    @SuppressWarnings("unchecked")
    private <T> T loadClassifierInstanceFromModel(Class<T> theClassifier, String inputFile) {
        T result;
        try {
            Constructor<T> meth = theClassifier.getDeclaredConstructor(new Class[]{String.class});
            //meth.setAccessible(true);
            result = meth.newInstance(inputFile);
        } catch (Exception e) {
            LOG.fatal("Error in loading a classifier from a model!");
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * Returns a classifier loaded with the given dataset.
     * @param <T>
     * @param theClassifier
     * @param inputFile
     */
    @SuppressWarnings("unchecked")
    private <T> T loadClassifierInstanceFromData(Class<T> theClassifier, JSONObject jsonDataSet) {
        T result;
        Constructor<T> meth;
        try {
            meth = theClassifier.getDeclaredConstructor(new Class[]{JSONObject.class});
        } catch (Exception e) {
            LOG.fatal("Error in getting getting a classifier for JSON data!");
            throw new RuntimeException(e);
        }
        try {
            result = meth.newInstance(jsonDataSet);
        } catch (Exception e) {
            // ideally would like to catch JSONException but we can't do that via reflection (not thrown directly)
            LOG.fatal("Error in loading JSON data for classifier (malformed JSON)!");
            throw new RuntimeException(e);
        }
        return result;
    }

    public void loadClassifierFromModel(String inputModelFileName) {
        this.classifierClass =
                (Class<? extends AbstractWekaCompleteCompositionVector>) this.getClass(CLASSIFIER, DEFAULT_CLASSIFIER);

        this.classifier = this.loadClassifierInstanceFromModel(classifierClass, inputModelFileName);
        //this.classAttribute = (Attribute) this.classifier.getAttributes().elementAt(this.classifier.getClassIndex());
    }

    public void loadClassifierFromData(JSONObject jsonDataSet) {
        this.classifierClass =
                (Class<? extends AbstractWekaCompleteCompositionVector>) this.getClass(CLASSIFIER, DEFAULT_CLASSIFIER);

        this.classifier = this.loadClassifierInstanceFromData(classifierClass, jsonDataSet);
    //this.classAttribute = (Attribute) this.classifier.getAttributes().elementAt(this.classifier.getClassIndex());
    }

    /**
     * Not the best way, but one way to set logging without changing the properties file. Also assumes log4j!
     *
     * @param level
     */
    public void setLoggingLevel(Level level) {
        // using log4j's Logger!
        Logger rootLogger = Logger.getRootLogger();
        LOG.info("Setting root logger to " + level.toString());
        rootLogger.setLevel(level);
    }

    private static void USAGE(String cli_title, HelpFormatter formatter, Options options) {
        System.out.println(cli_title);
        formatter.printHelp(APPLICATION_TITLE, options);
        System.out.printf("\nDefault classifier is %s\n", DEFAULT_CLASSIFIER.getName());
        System.out.printf("Usage:\njava -cp ccv.jar org.mitre.ccv.weka.CompositionCompositionVectorClassifiers " +
                "\n\t -D weka.classifier.class=org.mitre.ccv.weka.CompleteCompositionVectorSMO -dataset ccv_test.json\n");
    }

    /**
     * Min things needed:
     * 1) a json file with ccv params, nmers and vectors, and a fasta file with
     *    samples to classify
     * 2) a json file with ccv params and nmers, a classifier file, and a fasta file
     *    with samples to classify
     *
     * @param argv
     * @throws java.lang.Exception
     */
    @SuppressWarnings("static-access")
    public static void main(String[] argv) throws FileNotFoundException, JSONException {

        CompositionCompositionVectorClassifiers ccvc = new CompositionCompositionVectorClassifiers();

        /** JSON file with data set. */
        FileReader dataSetReader = null;

        /** Optional sequences to generate ccvs and classify. */
        FastaIterator seqIter = null;

        /** Optional filename to output the classifier to. */
        String outputModelFileName = null;

        /** Optional filename to input the classifer from. */
        String inputModelFileName = null;

        /** Optional filename (also need a JSON data set) to input sequence data from. */
        String inputDataFileName = null;

        /** Optional filename for the JSON data set containing features and window sizes. */
        String inputJsonFileName = null;

        /** If set, do cross-validation numFolds and quit. */
        Integer numFolds = null;

        /** How to parse the sample name */
        int nameParser = 1;

        /** Limit the max length of sequences processed */
        Integer limitLength = null;

        /** What we are */
        String cli_title = APPLICATION_TITLE + " [build: " + APPLICATION_BUILD +
                "]\n(C) Copyright 2007-2009, by The MITRE Corporation.\n";

        /** create the Options */
        Options options = new Options();
        options.addOption(
                OptionBuilder.withArgName("file").hasArg(true).withDescription("JSON file with ccv " +
                "parameters, nmers, and vectors for generating " +
                "the classifier").create("dataset"));

        options.addOption(
                OptionBuilder.withArgName("file").hasArg(true).withDescription("a saved classifier (must use the same classifier)" +
                "(dataset then only needs parameters and nmers. " +
                "Vectors are no longer needed generating) ").create("inputModel"));

        options.addOption(
                OptionBuilder.withArgName("file").hasArg(true).withDescription("Output the generated tree " +
                "to a file as binary" +
                "(optional)").create("outputModel"));

        options.addOption(
                OptionBuilder.withArgName("numFolds").hasArg(true).withDescription("Cross validate the generated model " +
                "using the given training set. No final model " +
                "will be outputed or samples classified. " +
                "(optional)").create("crossValidate"));

        options.addOption(
                OptionBuilder.withArgName("file").hasArg(true).withDescription("file with samples" +
                " to generate ccvs and classify (optional)").create("samples"));

        options.addOption(
                OptionBuilder.withArgName("number").hasArg(true).withDescription("what name to use: 1-name(default);2-description;3-full header(might break newick-style trees)").create("name"));
        
        options.addOption(
                OptionBuilder.withArgName("number").hasArg(true).withDescription("limits the max length of the input samples processed").create("limitLength"));

        options.addOption(
                OptionBuilder.withArgName("help").hasArg(false).withDescription("print this message").create("help"));
        options.addOption(
                OptionBuilder.withArgName("level").hasArg(true).withDescription("Set the logging (verbosity) level: OFF, FATAL, ERROR, WARN, INFO, DEBUG. TRACE, ALL (none to everything)").create("verbosity"));

        options.addOption(OptionBuilder.withArgName("property=value").hasArgs().withValueSeparator().withDescription("use value for given property").create('D'));

        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();

        // create the parser
        CommandLineParser parser = new GnuParser();

        // parse the command line arguments
        CommandLine line;
        try {
            line = parser.parse(options, argv);

            if (line.hasOption("help") || line.hasOption("h")) {
                USAGE(cli_title, formatter, options);
                System.exit(0);         // they asked for help
            }

            if (line.getOptions().length == 0) {
                USAGE(cli_title, formatter, options);
                System.exit(-1);        // we need some options
            }

            if (line.hasOption("verbosity")) {
                // if this fails then they get DEBUG!
                ccvc.setLoggingLevel(Level.toLevel(line.getOptionValue("verbosity")));
            }
            Option[] opts = line.getOptions();

            for (Option opt : opts) {
                if (opt.getOpt().equals("D")) {
                    String propertyName = opt.getValue(0);
                    String propertyValue = opt.getValue(1);
                    System.setProperty(propertyName, propertyValue);
                    continue;
                }

                if (opt.getOpt().equals("dataset")) {
                    inputJsonFileName = line.getOptionValue("dataset");
                    continue;
                }
                if (opt.getOpt().equals("inputModel")) {
                    inputModelFileName = line.getOptionValue("inputModel");
                    continue;
                }
                if (opt.getOpt().equals("outputModel")) {
                    outputModelFileName = line.getOptionValue("outputModel");
                    continue;
                }
                if (opt.getOpt().equals("samples")) {
                    inputDataFileName = line.getOptionValue("samples");
                    continue;
                }
                if (opt.getOpt().equals("crossValidate")) {
                    try {
                        numFolds =
                                Integer.parseInt(line.getOptionValue("crossValidate"));

                    } catch (NumberFormatException nfe) {
                        LOG.warn("Error while parsing number of fold cross validate", nfe);
                        System.exit(-1);
                    }
                    continue;
                }
                if (line.hasOption("name")) {
                    try {
                        nameParser =
                            Integer.parseInt(line.getOptionValue("name"));
                    } catch (NumberFormatException nfe) {
                        throw new ParseException(
                            "Error parsing 'name' option. Reason: " +
                            nfe.getMessage());
                    }
                }
                //"limitLength"
                if (line.hasOption("limitLength")) {
                    try {
                        limitLength =
                            Integer.parseInt(line.getOptionValue("limitLength"));
                    } catch (NumberFormatException nfe) {
                        throw new ParseException(
                            "Error parsing 'limitLength' option. Reason: " +
                            nfe.getMessage());
                    }
                }
            } // for opts
        } catch (ParseException pex) {
            System.err.println("Error while parse options\n" + pex.getMessage());
            USAGE(cli_title, formatter, options);
            System.exit(-1);
        }
        /** Print out who we are */
        LOG.info(cli_title);

        /** Do we have a saved model? */
        if (inputModelFileName != null && numFolds == null) {
            LOG.info(String.format("Loading previous model from '%s'", inputModelFileName));
            ccvc.loadClassifierFromModel(inputModelFileName);
            // we cannot cross-validate because we no longer have the instance data!
            // @todo: can we set the classifier class at the same time?
        } else if (inputJsonFileName != null) {
            /**
             * Load the dataset and process the sequecnes
             */
            LOG.info("Loading data set from '" + inputJsonFileName + "'");
            dataSetReader = new FileReader(inputJsonFileName);
            JSONObject jsonDataSet = new JSONObject(JsonLoader.getJsonString(dataSetReader));
            try {
                dataSetReader.close();
            } catch (IOException ioe) {
                LOG.warn("Error while closing file reader!");
            }

            ccvc.loadClassifierFromData(jsonDataSet);

            if (numFolds != null) {
                LOG.info(String.format(
                        "Cross-validating model %d fold using training data....", numFolds));
                ccvc.classifier.crossValidateModel(ccvc.classifier.getClassifier(),
                        ccvc.classifier.getInstances(), numFolds);
                /** Exit, we only do cross validation */
                LOG.info("Done.");
                System.exit(0);
            }

            /**
             * Train/Build the classifier - move this to a method
             */
            LOG.info(String.format("Building the %s classifier...", ccvc.classifier.getClassiferName()));
            try {
                ccvc.classifier.buildClassifier();
            } catch (Exception ex) {
                LOG.fatal("Error while building the classifier with the given dataset", ex);
                throw new RuntimeException();
            }

            /**
             * Save it?
             */
            if (outputModelFileName != null) {
                LOG.info("Saving classifier model to '" + outputModelFileName + "'");
                try {
                    ccvc.classifier.saveModelObject(outputModelFileName);
                } catch (IOException ioe) {
                    LOG.warn("IO error while saving model", ioe);
                } catch (Exception ex) {
                    LOG.fatal("Unknown error", ex);
                    throw new RuntimeException();
                }
            }
        } else {
            LOG.fatal("Unknown set of options!");
            System.out.println(cli_title);
            formatter.printHelp(cli_title + " options ", options);
            System.exit(-1);
        }

        /** Check to see if we loaded a classifier!*/
        if (ccvc.classifier == null) {
            LOG.fatal("Somehow we didn't load a classifier!");
            throw new RuntimeException();
        }

        /**
         * Do we have samples to classify?
         */
        if (inputDataFileName == null) {
            LOG.info("No samples to classify. Quiting.");
            return;
        }
        
        BufferedReader br = new BufferedReader(new FileReader(inputDataFileName));
        seqIter = new FastaIterator(br);
        if (seqIter != null) {

            /** Get the ccv begin and end  */
            Integer begin = ccvc.classifier.getBegin();
            Integer end = ccvc.classifier.getEnd();

            /**
             * no way to get FastVector of attirbutes from an Instances Object?
             */
            Attribute classAttribute = (Attribute) ccvc.classifier.getAttributes().elementAt(ccvc.classifier.getClassIndex());
            Integer cv = 0;

            /** Load and generate CCVs */
            while (seqIter.hasNext()) {
                cv++;
                Sequence s = null;
                try {
                    s = seqIter.next();
                } catch (NoSuchElementException nsee) {
                    //System.err.println("Iteration error in sequence file!\n " + e.getMessage());
                    LOG.fatal("Iteration error in sequence file!", nsee);
                    throw new RuntimeException();
                }
                String seqString = s.seqString();
                String seqName = CompleteCompositionVectorMain.parseSequenceName(s, nameParser);

                /** Check the length of the sequence */
                if (limitLength != null && seqString.length() > limitLength) {
                    LOG.info(String.format("Skipping sample '%s' because it is longer (%d) than the limit length (%d)",
                            seqName, seqString.length(), limitLength));
                    continue;

                }
                /** Generate the complete composition vector for this sample */
                IndexedCompositionDistribution cd;
                try {
                    cd = new IndexedCompositionDistribution(
                            new DistributionIndex(), cv, seqString, begin, end);
                } catch (IllegalArgumentException e) {
                    LOG.warn(String.format("Composition distribution error on '%s'" +
                            "(potentially to small of a sequence '%d')!", s.getName(), seqString.length()), e);
                    continue;
                }
                CompleteCompositionVector ccv = new IndexedCompleteCompositionVector(seqName, cv, begin, end, cd);

                /** Generate an instance for this */
                Instance inst;
                try {
                    inst = ccvc.classifier.getInstanceSparse(ccv);
                } catch (Exception ex) {
                    LOG.warn("Error while getting instance! Skipping...", ex);
                    continue;
                }

                /** To classify, we need to be part of a DataSet/Instance */
                try {
                    LabeledInstance li = ccvc.classifier.runClassifier(inst, classAttribute);
                    double[] clsDist = ccvc.classifier.getClassifier().distributionForInstance(inst);

                    int clsValue = (int) li.inst.classValue();
                    System.out.printf("%s\t%s\t%f\n", s.getName(),
                            classAttribute.value(clsValue), li.clsDist[clsValue]);

                    /* Below outputs all the confidence values for all the classes.
                     StringBuffer sb = new StringBuffer();
                    for (int i = 0; i < clsDist.length; i++) {
                        sb.append(clsDist[i]);
                        if (i + 1 < clsDist.length) {
                            sb.append(", ");
                        }
                    }
                     
                    System.out.printf("%s\t%s\t[%s]\n", seqName,
                            classAttribute.value(clsValue), sb.toString());
                     */
                } catch (Exception ex) {
                    LOG.warn(String.format("Error while classifying sample '%s' (# %d)", seqName, cv));
                }

            }
        }
    }
}

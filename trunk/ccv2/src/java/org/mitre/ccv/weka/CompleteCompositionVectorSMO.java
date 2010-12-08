/**
 * Sept 10, 2008
 * 
 * $Id$
 */
package org.mitre.ccv.weka;

import java.io.FileNotFoundException;import java.io.ObjectInputStream;
;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.json.JSONException;
import org.json.JSONObject;


import weka.classifiers.Classifier;
import weka.classifiers.functions.SMO;
//import weka.classifiers.functions.SMOreg; Can't handle nominal attributes, i.e. our classes
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

/**
 * A class that uses Weka's <code>SMO</code> (a SVM) classifier that classifies Complete Composition Vectors.
 *
 * <P> It seems that the SMO SVM class doesn't produce meaningful confidence (distrubutions).
 * <br> For example:
 * <pre>[0.13333333333333333, 0.2, 0.3333333333333333, 0.26666666666666666, 0.0, 0.06666666666666667]</pre>
 * which makes item 2 the best match.
 *
 * @author Marc Colosimo
 */
public class CompleteCompositionVectorSMO
        extends AbstractWekaCompleteCompositionVector {

    private static final Log LOG = LogFactory.getLog(CompleteCompositionVectorSMO.class);
    private Integer classIdx;
    private Instances instances = null;
    /** Our classifier */
    private SMO classifier = null;
    private Integer begin = null;
    private Integer end = null;
    /** */
    private LinkedList<String> nmers = null;
    /** The filter used to get rid of missing values. */
    //protected ReplaceMissingValues missing;

    public static final String CLASSIFIER_NAME = "Weka SMO";
    
    /**
     * Constructor that loads in data from a JSON file.
     *
     * @see {@link JsonLoader}
     * @param jsonDataSet
     * @throws org.json.JSONException
     * @throws java.io.IOException
     */
    public CompleteCompositionVectorSMO(JSONObject jsonDataSet)
            throws JSONException, IOException {
        JsonLoader loader = new JsonLoader(jsonDataSet);

        //this.instances = loader.getDataSet();
        this.setInstances(loader.getDataSet());
        if (instances.classIndex() == -1) {
            // Set the class instance to be the last attribute
            instances.setClassIndex(instances.numAttributes() - 1);
            this.setClassIndex(instances.numAttributes() - 1);
        } else {
            this.setClassIndex(instances.classIndex());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format(
                    "Have %d classes with %d attributes.",
                    this.instances.numClasses(), this.instances.numAttributes() - 1));
        }
        
        /** TODO: load classifier options from json */
        /** Get the ccv begin and end from the json file - move this */
        JSONObject jccv = jsonDataSet.getJSONObject("properties");
        this.begin = jccv.getInt("begin");
        this.end = jccv.getInt("end");
    }

    /**
     * Constructor that loads a saved classifier (only as a binary file).
     * @param inputFile
     * @throws java.io.FileNotFoundException
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException
     */
    public CompleteCompositionVectorSMO(String inputFile)
            throws FileNotFoundException, IOException, ClassNotFoundException {
        this.loadModelObject(inputFile);
    }

        /**
     * Constructor that loads a saved classifier (only as a binary file).
     * @param ois   stream to read from
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException
     */
    public CompleteCompositionVectorSMO(ObjectInputStream ois)
            throws IOException, ClassNotFoundException {
        this.loadModelObject(ois);
    }

    @Override
    public String getClassiferName() {
        return CLASSIFIER_NAME;
    }

    @Override
    public void setInstances(Instances instances) {
        this.instances = instances;
    }

    /**
     * Not a copy. This is the real instances data.
     */
    public Instances getInstances() {
        return this.instances;
    }

    /** 
     * Get the nmers in the order that they are attributes.
     * 
     * @return
     */
    public LinkedList<String> getNmers() {
        if (this.nmers == null) {
            this.nmers = new LinkedList<String>();
            FastVector attrs = this.getAttributes();
            for (int i = 0; i < attrs.size(); i++) {

                if (i == this.getClassIndex()) {
                    continue;
                }
                Attribute attr = (Attribute) attrs.elementAt(i);

                this.nmers.add(attr.name());
            }
        }
        return this.nmers;
    }

    @Override
    public void setNmers(LinkedList<String> nmers) {
        /* Check to see if we have labeled data and raise error */
        if (this.instances != null &&
                this.instances.enumerateAttributes().hasMoreElements()) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        this.nmers = nmers;
    }

    @Override
    public Integer getBegin() {
        return this.begin;
    }

    @Override
    public void setBegin(Integer begin) {
        this.begin = begin;
    }

    @Override
    public Integer getEnd() {
        return this.end;
    }

    @Override
    public void setEnd(Integer end) {
        this.end = end;
    }

    public void buildClassifier() throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
        /**String[] j48Options = new String[1];
        j48Options[0] = "-U";            // unpruned tree
        J48 tree = new J48();            // new instance of tree
        tree.setOptions(j48Options);            // set the options
         */
        SMO svm = (SMO) this.getClassifier();
        //tree.buildClassifier(this.instances);  // build classifier
        svm.buildClassifier(this.instances);
        this.setClassifier(svm);
    }

    /** Return our J48 classifier: this could have been trained. */
    public Classifier getClassifier() {
        if (this.classifier == null) {
            /** Try to make a new instance of a J48 classifier */
            String[] smoOptions = new String[2];
            /** 
             * For improved speed normalization should be turned off when 
             * operating on SparseInstances, which we are expecting to have.
             * 
             * Also, it seems to produce better results.
             */
            smoOptions[0] = "-N 0";         // neither (0)normalize or (1) standardize

            smoOptions[1] = "-K weka.classifiers.functions.supportVector.RegSMOImproved";
            //  -K <classname and parameters>
            // default: weka.classifiers.functions.supportVector.PolyKernel

            this.classifier = new SMO();    // new instance of the svm
            try {
                this.classifier.setOptions(smoOptions); // set the options
            } catch (Exception ex) {
                LOG.error("Unable to set options.", ex);
                this.classifier = null;
            }
        }
        return this.classifier;
    }

    public void setClassifier(Classifier smo) {
        this.classifier = (SMO) smo;
    }

    public Integer getClassIndex() {
        if (this.classIdx == null && this.instances == null) {
            return null; // Should this throw exception?
        }
        if (this.classIdx != null) {
            return this.classIdx;
        }
        this.classIdx = this.instances.classIndex();
        return this.classIdx;
    }

    public void setClassIndex(Integer idx) {
        this.classIdx = idx;
    }

    public Instance replaceMissing(Instance inst) {
        return null;
    }



    
}

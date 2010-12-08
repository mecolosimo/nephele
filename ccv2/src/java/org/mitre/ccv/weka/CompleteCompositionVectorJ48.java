/**
 * CompleteCompositionVectorJ48.java
 *
 * Created on Jul 28, 2008, 8:14:34 AM
 *
 * $Id$
 */
package org.mitre.ccv.weka;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import weka.classifiers.Classifier;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instances;

/**
 * A class that uses Weka's <code>J48</code> (a decision tree) classifier that classifies Complete Composition Vectors.
 * 
 * @author Marc Colosimo
 */
public class CompleteCompositionVectorJ48 
        extends AbstractWekaCompleteCompositionVector {

    private static final Log LOG = LogFactory.getLog(CompleteCompositionVectorJ48.class);

    /** 
     * Index of class attribute, usually instances.numAttribute - 1. 
     * We cannot just use a reference to the actual class Attribute 
     * because Weka copies the objects, so a.equals(classAttribute) doesn't work!
     * 
     * However, attribute.index() should give us the correct index. If it is in
     * Instances.
     */
    private Integer classIdx;
    
    private Instances instances = null;

    /** Our classifier */
    private J48 classifier = null;
    
    private Integer begin = null;
    
    private Integer end = null;
    
    /** */
    private LinkedList<String> nmers = null;

    public static final String CLASSIFIER_NAME = "Weka J48";
    
    public CompleteCompositionVectorJ48(JSONObject jsonDataSet) 
            throws JSONException, IOException {
        JsonLoader loader = new JsonLoader(jsonDataSet);

        //this.instances = loader.getDataSet();
        this.setInstances(loader.getDataSet());
        if (this.instances.classIndex() == -1) {
            // Set the class instance to be the last attribute
            this.instances.setClassIndex(this.instances.numAttributes() - 1);
            this.setClassIndex(this.instances.numAttributes() - 1);
        } else {
            this.setClassIndex(this.instances.classIndex());
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
    public CompleteCompositionVectorJ48(String inputFile) 
            throws FileNotFoundException, IOException, ClassNotFoundException 
    {
        this.loadModelObject(inputFile);
    }

    /**
     * Constructor that loads a saved classifier (only as a binary file).
     * @param ois   stream to read from
     * @throws java.io.IOException
     * @throws java.lang.ClassNotFoundException
     */
    public CompleteCompositionVectorJ48(ObjectInputStream ois)
            throws IOException, ClassNotFoundException
    {
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
     * Returns the <code>Instances</code>
     *
     * <p>This does <b>not</b> return a copy. This is the real instances data.
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
            //System.err.printf("Labeled Data has %d attributes\n", this.labeledData.numAttributes());
            this.nmers = new LinkedList<String>();
           //for (Enumeration e = this.instances.enumerateAttributes() ; 
            FastVector attrs = this.getAttributes();
            for (int i = 0; i < attrs.size(); i++) {
                //e.hasMoreElements() ;) {
                //Attribute attr = (Attribute) e.nextElement();
                if (i == this.getClassIndex())
                    continue;
                Attribute attr = (Attribute) attrs.elementAt(i);
                
                this.nmers.add(attr.name());
                //System.err.printf("%s, ", attr.name());
            }
        }
        //System.err.println();
        return this.nmers;
    }
    
    @Override
    public void setNmers(LinkedList<String> nmers) {
        /* Check to see if we have labeled data and raise error */
        if ( this.instances != null &&
             this.instances.enumerateAttributes().hasMoreElements() )
            throw new UnsupportedOperationException("Not supported yet.");
        this.nmers = nmers;
    }

    @Override
    public Integer getBegin() {
        return this.begin;
    }

    @Override
    public void setBegin(Integer begin) {
        this.begin  = begin;
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
        J48 tree = (J48) this.getClassifier();
        tree.buildClassifier(this.instances);  // build classifier
        this.setClassifier(tree);
    }
    
    /** Return our J48 classifier: this could have been trained. */
    public Classifier getClassifier() {
        if ( this.classifier == null ) {
            /** Try to make a new instance of a J48 classifier */
            String[] j48Options = new String[1];
            j48Options[0] = "-U";                       // unpruned tree
            this.classifier = new J48();                // new instance of tree
            try {
                this.classifier.setOptions(j48Options); // set the options
            } catch (Exception ex) {
                LOG.fatal("Unable to set classifier options!",ex);
                this.classifier = null;
            }
        }
        return this.classifier;
    }

    public void setClassifier(Classifier j48) {
        this.classifier = (J48) j48;
    }
    
    public Integer getClassIndex() {
        if (this.classIdx == null && this.instances == null) 
            return null; // Should this throw exception?
        if (this.classIdx != null)
            return this.classIdx;
        this.classIdx = this.instances.classIndex();
        return this.classIdx;
    }
    
    public void setClassIndex(Integer idx) {
        this.classIdx = idx;
    }

}

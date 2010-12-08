/**
 * JsonLoader.java
 *
 * Created on Jul 22, 2008, 9:16:15 AM
 *
 * $Id$
 */
package org.mitre.ccv.weka;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;

/**
 * Reads a JSON file produced by the CCV code. This is based on weka 3.5.7 code.
 * This class should be easy to adapt into the weka.core.converters style of 3.5
 * 
 * Most of this belongs under JsonReader if one was to follow the weka style.
 * 
 * @author Marc Colosimo
 */
public class JsonLoader {

    private static final Log LOG = LogFactory.getLog(JsonLoader.class);
    /** Holds the determined structure (header) of the data set. */
    protected Instances m_structure = null;
    /** Buffer of values for sparse instance */
    protected double[] m_ValueBuffer;
    /** Buffer of indices for sparse instance */
    protected int[] m_IndicesBuffer;
    /** the actual data */
    protected Instances m_Data = null;
    /**
     * Current index in the features data.
     */
    private Integer m_featureIdx = null;

    public JsonLoader(Reader reader) throws IOException, JSONException {
        init(new JSONObject(this.getJsonString(reader)));
    }

    public JsonLoader(JSONObject jsonData) throws JSONException {
        init(jsonData);
    }

    /**
     * Determines and returns (if possible) the structure (internally the 
     * header) of the data set as an empty set of instances.
     *
     * @return the structure of the data set as an empty set of Instances
     * @exception IOException if an error occurs
     */
    public Instances getStructure() throws IOException {
        // this is the JsonReader code, not JsonLoader code to be
        // Why copying the data?
        return new Instances(this.m_Data, 0);
    }

    /**
     * Return the full data set. If the structure hasn't yet been determined
     * by a call to getStructure then method should do so before processing
     * the rest of the data set.
     *
     * @return the structure of the data set as an empty set of Instances
     * @exception IOException if there is no source or parsing fails
     */
    public Instances getDataSet() throws IOException {
        // thisis the JsonReader code, not JsonLoader code to be
        return this.m_Data;
    }

    private void init(JSONObject jsonData) throws JSONException {
        /** Get our features/attributes */
        JSONArray ja = jsonData.getJSONArray("features");
        FastVector attributes = this.getAttributes(ja);

        /** Get clusters/classes as attributes */
        ja = jsonData.getJSONArray("clusters");
        FastVector clusterValues = this.getClasses(ja);
        Attribute classAttr = new Attribute("classes", clusterValues, attributes.size());
        attributes.addElement(classAttr);

        ja = jsonData.getJSONArray("samples");  // was sample
        int capacity = ja.length() + 1;

        /** Get relation name, move this into a Weka JSON object. */
        String relationName = "ccv";
        if (jsonData.has("relationName")) {
            relationName = jsonData.getString("relationName");
        }
        this.m_Data = new Instances(relationName, attributes, capacity);
        this.m_Data.setClass(classAttr);
        initBuffers();

        /** Read in the sparse data */
        Instance current = getInstanceSparse(ja);
        while (current != null) {
            this.m_Data.add(current);
            current = getInstanceSparse(ja);
        }

        compactify();
    }

    /**
     * initializes the buffers for sparse instances to be read
     * 
     * @see			#m_ValueBuffer
     * @see			#m_IndicesBuffer
     */
    protected void initBuffers() {
        this.m_ValueBuffer = new double[this.m_Data.numAttributes()];
        this.m_IndicesBuffer = new int[this.m_Data.numAttributes()];
    }

    /**
     * compactifies the data
     */
    protected void compactify() {
        if (this.m_Data != null) {
            this.m_Data.compactify();
        }
    }

    /**
     * Reads a sparse instance from the JSONArray.
     *
     * @param ja a JSONArray representing the sample
     * @return an Instance or null if there are no more instances to read
     * @exception IOException if an error occurs
     */
    private Instance getInstanceSparse(JSONArray ja)
            throws JSONException {
        int valIndex, numValues = 0, maxIndex = -1;

        /** Did we start and finish or never started? */
        if (this.m_featureIdx == null) {
            this.m_featureIdx = 0;
        } else if (this.m_featureIdx >= ja.length()) {
            return null;
        }

        JSONObject jo = ja.getJSONObject(this.m_featureIdx);
        this.m_featureIdx++;

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Reading sample %s", jo.getString("name")));
        }
        
        /** 
         * Object keys not guaranteed to be in order, which they need to be.
         * However, they are stored as Strings and are sorted as Strings
         * (not as numbers).
         */
        JSONObject data = jo.getJSONObject("data");
        ArrayList<Integer> dl = new ArrayList<Integer>(data.length());
        for (Iterator keys = data.sortedKeys(); keys.hasNext();) {
            dl.add(Integer.valueOf((String) keys.next()));
        }
        Collections.sort(dl);

        for (Integer ikey : dl) {
            String key = ikey.toString();
            try {
                this.m_IndicesBuffer[numValues] = Integer.valueOf(key);
            } catch (NumberFormatException ex) {
                LOG.fatal(ex);
                throw new JSONException("Index number expected at Instance " + this.m_featureIdx.toString() + " found '" + key + "'");
            }
            if (this.m_IndicesBuffer[numValues] <= maxIndex) {
                throw new JSONException("Indices have to be ordered for Instance " + this.m_featureIdx.toString());
            }
            if ((this.m_IndicesBuffer[numValues] < 0) ||
                    (this.m_IndicesBuffer[numValues] >=
                    this.m_Data.numAttributes())) {
                throw new JSONException("Index out of bounds for Instance " + this.m_featureIdx.toString());
            }
            maxIndex = this.m_IndicesBuffer[numValues];

            /** Now get the value. */
            Double value = data.getDouble(key);

            /** We don't check the type since we expect only one type */
            this.m_ValueBuffer[numValues] = value;

            numValues++;
        }

        /** Get nomimal clusters/classes which are the last index. */
        this.m_IndicesBuffer[numValues] = this.m_Data.numAttributes() - 1;
        String cluster = jo.getString("cluster");
        /** Check if value appears in header. */
        //valIndex = this.m_Data.attribute("classes").indexOfValue(cluster);
        valIndex = this.m_Data.attribute(this.m_IndicesBuffer[numValues]).
                indexOfValue(cluster);
        //System.err.printf("%s with class %s (index = %d)\n", 
        //        jo.getString("name"),cluster, valIndex);
        if (valIndex == -1) {
            throw new JSONException("Nominal value '" + cluster + "' not " +
                    "declared in header for Instance " + this.m_featureIdx.toString());
        }
        this.m_ValueBuffer[numValues] = (double) valIndex;
        numValues++;

        /** some magic copying to add this instance to the dataset */
        double[] tempValues = new double[numValues];
        int[] tempIndices = new int[numValues];
        System.arraycopy(m_ValueBuffer, 0, tempValues, 0, numValues);
        System.arraycopy(m_IndicesBuffer, 0, tempIndices, 0, numValues);
        Instance inst = new SparseInstance(1, tempValues, tempIndices,
                m_Data.numAttributes());
        inst.setDataset(m_Data);

        return inst;
    }

    /**
     * Reads attributes (features) from the JSON Array. All of our attributes are
     * 'NUMERIC'.
     * 
     * @param ja
     * @return the new attributes vector
     */
    private FastVector getAttributes(JSONArray ja) {
        FastVector attributes = new FastVector();
        try {
            for (int idx = 0; idx < ja.length(); idx++) {
                String attributeName = ja.getString(idx);
                attributes.addElement(new Attribute(attributeName, attributes.size()));
            }
        } catch (JSONException ex) {
            LOG.warn(ex.getMessage());
        }
        return attributes;
    }

    /**
     * Reads in the class features (nominal attributes) from the JSON Array.
     * 
     * @param ja
     * @return the nominal attributes vector
     */
    private FastVector getClasses(JSONArray ja) {
        FastVector attributeValues = new FastVector();
        try {
            for (int idx = 0; idx < ja.length(); idx++) {
                String attributeValue = ja.getString(idx);
                attributeValues.addElement(attributeValue);
            }
        } catch (JSONException ex) {
            LOG.warn(ex.getMessage());
        }
        return attributeValues;
    }

    /** JSON doesn't take Readers! */
    protected static String getJsonString(Reader reader) {
        BufferedReader in = new BufferedReader(reader);
        StringBuffer r = new StringBuffer();
        try {
            String s = in.readLine();
            while (s != null) {
                r.append(s);
                s = in.readLine();
            }
        } catch (IOException ex) {
            LOG.warn(ex.getMessage());
        }
        return r.toString();
    }

    /**
     * Main method.
     *
     * @param args should contain the name of an input file.
     */
    public static void main(String[] args) {
        if (args.length > 0) {
            JsonLoader loader;
            try {
                loader = new JsonLoader(new FileReader(new File(args[0])));
                //Instances structure = loader.getStructure();
                //System.out.println(structure);

                Instances data = loader.getDataSet();

                /** setting class attribute, we load these last */
                // setting class attribute if the data format does not provide this information
                // E.g., the XRFF format saves the class attribute information as well
                if (data.classIndex() == -1) {
                    data.setClassIndex(data.numAttributes() - 1);
                }

                /** Save the json file as an arff file-type */
                if (args.length > 1) {
                    BufferedWriter writer = new BufferedWriter(
                            new FileWriter(args[1]));
                    writer.write(data.toString());
                    writer.newLine();
                    writer.flush();
                    writer.close();
                }

                String[] options = new String[1];
                options[0] = "-U";            // unpruned tree
                J48 tree = new J48();         // new instance of tree

                // new instance of tree
                tree.setOptions(options); // set the options

                Evaluation eval = new Evaluation(data);
                eval.crossValidateModel(tree, data, 10, new Random(1));
                System.out.println(eval.toSummaryString("\nResults\n=======\n", true));
                System.out.println(eval.toClassDetailsString());

            } catch (IOException ex) {
                LOG.warn(ex.getMessage());
            } catch (JSONException ex) {
                LOG.warn(ex.getMessage());
            } catch (Exception ex) {
                LOG.warn(ex.getMessage());
            }

        }
    }
}

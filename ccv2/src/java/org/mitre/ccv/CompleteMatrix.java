/**
 * CompleteMatrix.java
 *
 * Created on Jun 27, 2008, 9:59:34 AM
 *
 * $Id$
 *
 * Changes from  Jun 27, 2008
 * -------------------------
 * 8-Sept-2009 : Rewrote to use jackson json package, plus added ability to read in json files
 * 20-Oct-2009 : Rewrote to use Apache Math instead of weka.matrix.Matrix
 * 15-Dec-2009 : Added getVectors<RealVector>
 */

package org.mitre.ccv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.commons.math.linear.RealMatrix;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math.linear.ArrayRealVector;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import org.mitre.math.linear.RealMatrixUtils;

/**
 * Wrapper class for the {@link VectorSet} processed nmers as a Weka 2-D Matrix that holds the axis lables
 * (nmers and sample names). Also holds information used to generate the
 * the matrix (begin, end, etc.)
 * 
 * @author Marc Colosimo
 */
public class CompleteMatrix {

    private static final Log LOG = LogFactory.getLog("CompleteMatrix");

    private RealMatrix matrix;
    private ArrayList<String> nmerList;
    private ArrayList<String> nameList;
    
    private Integer begin;
    private Integer end;

    static private ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally

    /**
     * Construct a CompleteMatrix
     * 
     * @param nmers in order that they are in the Matrix
     * @param names in order that they are in the Matrix
     * @param matrix {@link RealMatrix} with samples stored as nmers(rows) by vectors (columns)
     */
    public CompleteMatrix(Integer begin, Integer end,
            ArrayList<String> nmers, ArrayList<String> names, 
            RealMatrix matrix) throws IllegalArgumentException
    {
        /** Should add code to check for the dimensions */
        this.matrix = matrix;
        this.nmerList = nmers;
        this.nameList = names;
        
        /** Should add code to check that end is after begin and begin >= 3 */
        if( begin <= 2 ) 
            throw new IllegalArgumentException(
                    "Value of 'begin' argument must be larger than 2!");
        if( end < begin  )
            throw new IllegalArgumentException(
                    "Value of 'end' argument must be larger than 'begin'!");
        this.begin = begin;
        this.end = end;
    }
    
    /**
     * Returns the underlining {@link RealMatrix}
     */
    public RealMatrix getMatrix() {
        return this.matrix;
    }
    
    /**
     * Returns the nmers in the order that they are in the matrix
     */
    public ArrayList<String> getNmers() {
        return this.nmerList;
    }
    
    /**
     * Returns the sample names in the order that they are in the matrix
     */
    public ArrayList<String> getNames() {
        return this.nameList;
    }

    /** Get double vaule from Weka Matrix */
    public double get(int i, int j) {
        return this.matrix.getEntry(i, j);
    }
    
    /** 
     * Get the begining length of the k-mers
     */ 
    public Integer getBegin() {
        return this.begin;
    }
    
    /**
     * Get the ending length of the k-mers
     */
    public Integer getEnd() {
        return this.end;
    }

    /**
     * Returns the samples as {@link RealVector}s.
     */
    public List<RealVector> getVectors() {
        ArrayList<RealVector> vectors = new ArrayList<RealVector>();
        /** rows (j) are the nmers, columns (i) are the sample vectors */
        final RealMatrix realMatrix = this.getMatrix();
        final int rows = realMatrix.getRowDimension();
        for (int n=0; n < realMatrix.getColumnDimension(); n++) {
            vectors.add(realMatrix.getColumnVector(n));
        }
        return vectors;
    }

    /**
     * Write out the vectors as sparse vectors to the file given.
     * <br>
     * This also writes out the properties used to generate the vectors and
     * the full set of features (nmers).
     * { 
     *      "properties" : 
     *      {
     *          "begin" : 3 
     *          "end"   : 9
     *      }  
     *      "features" : [..]
     *      "samples" : 
     *      [ 
     *          {   
     *              "name" : "sample name",
     *              "data" : { nmer_index: non-zero pi-values }
     *          }, ....
     *      ]
     * }
     * 
     * @return the JSONObject representing this class
     */
    public void writeJsonCompleteMatrix(BufferedWriter bw) throws JsonGenerationException, IOException {
        JsonFactory jsonFactory = new JsonFactory();
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(bw);

        jsonGenerator.writeStartObject();
        this.writeJsonCcvProperties(jsonGenerator);
        this.writeJsonCcvFeatures(jsonGenerator);
        this.writeJsonCcvVectors(jsonGenerator);
        jsonGenerator.writeEndObject();
        jsonGenerator.flush();
    }

    /**
     * Writes out just the features (nmers):
     *
     * Format:
     * 
     * "features" : [..]
     *
     * @param bw
     * @throws JsonGenerationException
     * @throws IOException
     */
    public void writeJsonNmers(BufferedWriter bw) throws JsonGenerationException, IOException {
        JsonFactory jsonFactory = new JsonFactory();
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(bw);
        jsonGenerator.writeStartObject();
        this.writeJsonCcvFeatures(jsonGenerator);
        jsonGenerator.writeEndObject();
        jsonGenerator.flush();
    }

    /**
     * Reads in the vectors from a json file.
     *
     * @see CompleteMatrix#writeJsonCompleteMatrix(java.io.BufferedWriter) 
     * @param br
     * @return
     * @throws JsonParseException
     * @throws IOException
     */
    public static CompleteMatrix readJsonCompleteMatrix(BufferedReader br) throws JsonParseException, IOException  {
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jsonParser = jsonFactory.createJsonParser(br);
        JsonNode propertiesNode = null;

        // need this since we are not guaranteed to get the samples after the names and nmers lists
        HashMap<Integer, HashMap<Integer, Double>> sampleMap = new HashMap<Integer, HashMap<Integer, Double>>();
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<String> nmers = new ArrayList<String>();

        JsonToken token = jsonParser.nextToken();
        if (token != JsonToken.START_OBJECT) {
            throw new JsonParseException("Expecting the start of a json object! Found '" + jsonParser.getText() + "'",
                    jsonParser.getCurrentLocation());
        }
        token = jsonParser.nextToken();
        while (token != JsonToken.END_OBJECT) {
            String fieldName = jsonParser.getCurrentName();
            if ("properties".equalsIgnoreCase(fieldName)) {
                jsonParser.nextToken();
                propertiesNode = mapper.readTree(jsonParser);
                // now at the next token after the END_OBJECT token
            } else if ("features".equalsIgnoreCase(fieldName)) {
                if (jsonParser.nextToken() == JsonToken.START_ARRAY) {
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        nmers.add(jsonParser.getText());
                    }
                } else {
                    LOG.warn("Expecting an array for feature list but did not find one!");
                }
            } else if ("samples".equalsIgnoreCase(fieldName)) {
                CompleteMatrix.readJsonCcvVectors(jsonParser, names, sampleMap);
            } else {
                LOG.debug("Unknown field '" + fieldName + "' encountered in parsing json file!");
            }
            token = jsonParser.nextToken();
        }

        // populate matrix by unrolling it from the sampleMap
        RealMatrix jmatrix = RealMatrixUtils.getNewRealMatrix(nmers.size(), names.size());
        for(Entry<Integer, HashMap<Integer, Double>> s: sampleMap.entrySet()) {
            int n = s.getKey();   // unbox once here
            for(Entry<Integer, Double> e : s.getValue().entrySet()) {
                jmatrix.setEntry(e.getKey(), n, e.getValue());
            }
        }
        sampleMap.clear();   // release what we can

        return new CompleteMatrix(
                propertiesNode.get("begin").getIntValue(),
                propertiesNode.get("end").getIntValue(),
                nmers, names, jmatrix);
    }

    /**
     * Writes out the features (the nmers found) as a JSONArray.
     *
     * "features" : [..]
     */
    private void writeJsonCcvFeatures(JsonGenerator jsonGenerator) throws JsonGenerationException, IOException {
        jsonGenerator.writeArrayFieldStart("features");
        for(String nmer : this.getNmers()) {
            jsonGenerator.writeString(nmer);
        }
        jsonGenerator.writeEndArray();
    }

    /** 
     * Writes out our properties as a JSONObject.
     *
     * Format:
     * "properties" : {
     *      "begin"     : integer
     *      "end"       : integer
     *      "topNmers"  : integer (or null/empty) <optional>
     * }
     */
    private void writeJsonCcvProperties(JsonGenerator jsonGenerator) throws JsonGenerationException, IOException {
        jsonGenerator.writeObjectFieldStart("properties");
        jsonGenerator.writeNumberField("begin", this.getBegin());
        jsonGenerator.writeNumberField("end", this.getEnd());
        if (this.getNmers() != null) {
            jsonGenerator.writeNumberField("topNmers", this.getNmers().size());
        }
        jsonGenerator.writeEndObject();
    }
    
    /**
     * Writes our vectors as JSONObjects in a JSONArray as sparse arrays (only non-zeros)
     * Format:
     * "samples" : [
     *      {   
     *          "name" : "sample name",
     *          "data" : { nmer_index: non-zero pi-values }
     *      }, ....
     * ]
     * 
     * nmer_index starts at 0 (zero)
     *
     * @param jsonGenerator the generator to use
     */
    public void writeJsonCcvVectors(JsonGenerator jsonGenerator) throws JsonGenerationException, IOException {
        jsonGenerator.writeArrayFieldStart("samples");
        /** rows (j) are the nmers, columns (i) are the sample vectors */
        RealMatrix wekaMatrix = this.getMatrix();
        Integer nameSize = this.getNames().size();
        for (int c = 0; c < nameSize; c++) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("name", this.getNames().get(c));
            jsonGenerator.writeObjectFieldStart("data");
            for (int r = 0; r < wekaMatrix.getRowDimension(); r++ ) {
                Double pi_value = wekaMatrix.getEntry(r, c);
                if (pi_value != 0.0) {

                    jsonGenerator.writeNumberField(Integer.toString(r), pi_value);
                } 
            }
            jsonGenerator.writeEndObject();     // end of our data/vector
            jsonGenerator.writeEndObject();     // end of our sample
        }
        jsonGenerator.writeEndArray();
    }

    /**
     * Reads in the the vectors/samples.
     *
     * expects currentToken to be first the START_ARRAY token after "samples" field name
     * when returning, currentToken is first token after the END_ARRAY token
     *
     * @see writeJsonCcvVectors
     */
    static private void readJsonCcvVectors(JsonParser jsonParser, ArrayList<String> nameList, HashMap<Integer, HashMap<Integer, Double>> sampleMap ) throws JsonParseException, IOException {
        Integer nameCnt = 0;
        if (jsonParser.nextToken() != JsonToken.START_ARRAY) {
            throw new JsonParseException("Expecting the start of an array found '" + jsonParser.getText() + "'!", jsonParser.getCurrentLocation());
        }
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            if (jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
                String name = null;
                HashMap<Integer, Double> vector = new HashMap<Integer, Double>();
                while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                    String namefield = jsonParser.getCurrentName();
                    jsonParser.nextToken();             // advance to next token
                    if ("name".equalsIgnoreCase(namefield)) {
                        name = jsonParser.getText();    // getText not getCurrentName
                    } else if ("data".equalsIgnoreCase(namefield)) {
                        Integer idx;
                        Double value;
                        while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                            idx = Integer.parseInt(jsonParser.getCurrentName());
                            jsonParser.nextToken();
                            value = jsonParser.getDoubleValue();
                            vector.put(idx, value);
                        }
                    } else {
                        LOG.debug(new JsonParseException("Unrecognized name field '" + namefield + "'", jsonParser.getCurrentLocation()));
                    }
                }
                nameList.add(name);
                sampleMap.put(nameCnt, vector);
                nameCnt += 1;
            } else {
                throw new JsonParseException("Expecting the start of an json object while parsing the vectors but found '"
                        + jsonParser.getText() + "'", jsonParser.getCurrentLocation());
            }
        }
    }
}

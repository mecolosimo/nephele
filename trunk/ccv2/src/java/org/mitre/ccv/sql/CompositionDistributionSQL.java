/**
 * EmbeddedCompositionDistributionSQL.java
 *
 * Created on Jan 7, 2008, 1:33:57 PM
 *
 * $Id$
 */
package org.mitre.ccv.sql;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.mitre.ccv.CompositionDistribution;
import org.mitre.ccv.CompositionDistributionMap;

/**
 * A CompositionDistribution class backed by a SQL database.
 * 
 * @author Marc Colosimo
 */
public class CompositionDistributionSQL implements CompositionDistribution {

    private Connection conn;
    private Integer cdId;
    private Integer vsId;
    
    /** Length of the sequence(s) that was used to generate the distribution.  */
    private Integer seqLength;
    private Integer start;
    private Integer stop;
    
    /** Map to hold our distribution Maps */
    private TreeMap<Integer, CompositionDistributionMap> countMap;

     /** Select the length of the underlining sequence. 
    private final static String getLengthStmt = 
            "SELECT seq_length FROM comp_dist_t WHERE cd_id = ?";
    */
    
     /** Update the length of the underlining sequence. */
    private final static String updateLengthStmt =
            "UPDATE comp_dist_t SET seq_length = seq_length + ? " +
            "WHERE cd_id = ?";
     
    private final static String getCompDistStmt = 
            "SELECT vst.vs_id, start_window_size, stop_window_size, seq_length, seq_name " +
            "FROM vector_set_t vst JOIN comp_dist_t cdt " +
            "ON (vst.vs_id = cdt.vs_id) " +
            "WHERE cdt.cd_id = ? ";
    
    /**
     * Construct a new <tt>CompositionDistributionSQL</tt> object using a
     * stored composition distribution.
     * 
     * @param compositionDistributionID the unique ID in the database
     * @param conn the database connection
     */
    public CompositionDistributionSQL(
            Integer compositionDistributionID,
            Connection conn) {
        this.conn = conn;
        this.cdId = compositionDistributionID;
        this.countMap = new TreeMap<Integer, CompositionDistributionMap>();
        this.vsId = null;
        
        /** Check to see if we exist and get our vsId. */
        PreparedStatement selectStmt = null;
        try {
            selectStmt = conn.prepareStatement(getCompDistStmt);
            selectStmt.setInt(1, cdId);
            ResultSet rs = selectStmt.executeQuery();
            if( rs.next() ) {                
                this.vsId = rs.getInt(1);
                this.start = rs.getInt(2);
                this.stop = rs.getInt(3);
                this.seqLength = rs.getInt(4);
                System.err.printf("CompositionDistributionSQL: " +
                        "Found '%s' data in the database.\n", rs.getString(5));

            } else 
                throw new SQLException("Could not find a VectorSet ID!");
            rs.close();

            /** If we are loading this in, we don't save the length and this
             * isn't part of the inferface anymore
            this.seqLength = this.getSequence().length();

             */
            
            /** Get or generate our composition distributions */
            for (int i = start - 2; i <= stop; i++) {
                this.countMap.put(i,
                        new CompositionDistributionSQLMap(i, this.cdId, this.conn));
            }

        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                selectStmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }  
    }
    
    /**
     * Construct a new <tt>CompositionDistributionSQL</tt> object using
     * the given sequence for a specific VectorSet.
     * 
     * @param seqName
     * @param seq
     * @param vectorSetID
     * @param conn
     */
    public CompositionDistributionSQL(
            String seqName, String seq,
            Integer vectorSetID, Connection conn) {

        this.conn = conn;
        this.vsId = vectorSetID;
        this.countMap = new TreeMap<Integer, CompositionDistributionMap>();
        this.seqLength = seq.length();

        initCompositionDistribution(seqName);
        
        /** Generate Composition Distributions */
        System.err.printf("CompositionDistributionSQL: Calculating composition distributions for %s\n", seqName);
        for (int i = start - 2; i <= stop; i++) {
            this.countMap.put(i,
                    new CompositionDistributionSQLMap(i, seq, this.cdId, this.conn));
        }
    }

    /**
     * Construct a new <tt>CompositionDistributionSQL</tt> object by 
     * storing another CompositionDistribution object.
     * 
     * @param seqName
     * @param vectorSetID
     * @param cd
     * @param conn
     */
    public CompositionDistributionSQL(String seqName, Integer vectorSetID, 
            CompositionDistribution cd, Connection conn) {
        
        this.conn = conn;
        this.vsId = vectorSetID;
        this.countMap = new TreeMap<Integer, CompositionDistributionMap>();
        this.seqLength = cd.length();

        initCompositionDistribution(seqName);
        
        /** Generate Composition Distributions */
        System.err.printf("CompositionDistributionSQL: Calculating composition distributions for %s\n", seqName);
        for (int i = start - 2; i <= stop; i++) {
            /** DEBUG: need to change Interface to export a Map of distributions */
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            CompositionDistributionMap cdm = cd.getDistribution(i);
            for (Iterator<String> iter = 
                    cdm.iterator(); iter.hasNext(); ) {
                String nmer = iter.next();
                map.put(nmer, cdm.get(nmer));
            }
            this.countMap.put(i,
                    new CompositionDistributionSQLMap(i, map, this.cdId, this.conn));
        }
    }
    
    /**
     * Add the given sequence to this distribution.
     * 
     * This <B>DOES NOT</B> handle the boundary cases between 
     * the two sequences.
     * 
     * @param inSequence sequence string to add
     */
    public void addSequence(String inSequence) {
        //System.err.println("SeqCompDist.addSequence ");
        for (Iterator<Entry<Integer, CompositionDistributionMap>> iter = this.countMap.entrySet().iterator(); iter.hasNext();) {
            Entry<Integer, CompositionDistributionMap> entry = iter.next();
            getDistribution(inSequence, entry.getValue(), entry.getKey());
        }
        this.seqLength += inSequence.length();
        
        /** Update the version in the table */
        PreparedStatement updateStmt = null;
        try {
            updateStmt = conn.prepareStatement(updateLengthStmt);
            updateStmt.setInt(1, inSequence.length());
            updateStmt.setInt(2, this.cdId);
            updateStmt.executeUpdate();

        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            /** Release all open resources to avoid unnecessary memory usage. */
            try {
                updateStmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
    }

    public void addDistribution(CompositionDistribution scd) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public CompositionDistributionMap getDistribution(Integer windowSize) {
        return this.countMap.get(windowSize);
    }

    public Integer length() {
        return this.seqLength;
    }

    /**
     * For single sequence:
     *      L - k + 1
     * where k = the windowSize.
     * 
     */
    public Integer getTotalSubStrings(Integer windowSize) {
        return this.seqLength - windowSize + 1;
    }

    public Integer startingWindowSize() {
        return this.start;
    }

    public Integer endingWindowSize() {
        return this.stop;
    }

    /**
     * Return an iterator for the window sizes.
     */
    public Iterator<Integer> iterator() {
        return this.countMap.keySet().iterator();
    }

    /**
     * Returns the composition distribution table id.
     */
    public Integer getId() {
        return this.cdId;
    }
    
    /**
     * Initalize the composition distribution table. We need a seqLength 
     * before calling this!
     * 
     * @param seqName
     */
    private void initCompositionDistribution(String seqName) {
        
        PreparedStatement insertStmt = null;
        PreparedStatement selectStmt = null;
        try {
            /** Add this data to the table. */
            insertStmt = conn.prepareStatement(
                    "INSERT INTO comp_dist_t(vs_id, seq_name, seq_length)" +
                    "VALUES (?, ?, ? )");
            insertStmt.setInt(1, this.vsId);
            insertStmt.setString(2, seqName);
            insertStmt.setInt(3, this.seqLength);
            insertStmt.executeUpdate();

            /** Get our id */
            selectStmt = conn.prepareStatement(
                    "SELECT MAX(cd_id) FROM comp_dist_t");
            ResultSet rs = selectStmt.executeQuery();
            if (rs.next()) {
                this.cdId = rs.getInt(1);
            } else // Make our own exception class?
            {
                throw new SQLException("Unable to retrieve cd_id!");
            }

            /** Get start/stop */
            selectStmt.close();
            selectStmt = conn.prepareStatement(
                    "SELECT start_window_size, stop_window_size " +
                    "FROM vector_set_t WHERE vs_id = ?");
            selectStmt.setInt(1, this.vsId);

            rs = selectStmt.executeQuery();
            if (rs.next()) {
                this.start = rs.getInt(1);
                this.stop = rs.getInt(2);
            } else { // Make our own exception class?
                throw new SQLException("Unable to retrieve start/stop!");
            }
            rs.close();
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            /** Release all open resources to avoid unnecessary memory usage. */
            try {
                insertStmt.close();
                selectStmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
    }
    
    /**
     * Populate a hash map with the counts for each substring
     * 
     * @param sequence the sequence
     * @param scdMap a distribution map (null is allowed)
     * @param length the of the nmer to generate counts for
     */
    private CompositionDistributionMap getDistribution(String sequence,
            CompositionDistributionMap cdMap,
            int windowSize)
            throws IllegalArgumentException {

        if (cdMap == null) {
            cdMap = new CompositionDistributionSQLMap(windowSize, sequence, this.cdId, this.conn);

        } else {
            if (cdMap.getWindowSize() != windowSize) {
                throw new IllegalArgumentException(
                        "CompositionDistributionSQL.getDistribution: " +
                        "different window sizes for Distributions!");
            }
            cdMap.addSequence(sequence);
        }

        return cdMap;
    }
}

 /**
 * CompositionDistirbutionSQLMap.java
 *
 * Created on Jan 8, 2008, 11:06:22 AM
 *
 * $Id$
 */

package org.mitre.ccv.sql;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import org.mitre.ccv.CompositionDistributionMap;

/**
 *
 * @author Marc Colosimo
 */
public class CompositionDistributionSQLMap implements CompositionDistributionMap {

    private Integer windowSize;
    private Connection conn;
    
    /** The comp_dist_id. */
    private Integer cdId;
    
    private static final String selectIteratorStmt =     
                    "SELECT nmer FROM comp_dist_map_t " +
                    "WHERE cd_id = ? AND window_size = ? " +
                    "ORDER BY nmer " ;
    
    private static final String insertCntStmt =
                    "INSERT INTO comp_dist_map_t(" +
                    "cd_id, window_size, nmer, cnt) " +
                    "VALUES (?, ?, ?, ?)";
    
    /**
     * Construct a new <tt>CompositionDistributionSQLMap</tt> object by loading in 
     * the given distribution map table. 
     * <br>
     * This <B>DOES NOT</B> check if the table entry exists!
     * 
     * @param windowSize
     * @param cdId
     * @param conn
     */
    public CompositionDistributionSQLMap(Integer windowSize, 
                Integer compositionDistributionID, Connection conn) {
        this.cdId = compositionDistributionID;
        this.conn = conn;
        this.windowSize = windowSize;
        
        /** Check to see if we have a composition */
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(
                    "SELECT cd_id FROM  comp_dist_map_t " +
                    "WHERE cd_id = ? AND window_size = ? ");
            stmt.setInt(1, this.cdId);
            stmt.setInt(2, this.windowSize);
            ResultSet rs = stmt.executeQuery();
            if( !rs.next() ) {
                /** Haven't stored anything! Try getting sequence */
                System.err.println("CompositionDistributionSQLMap: " +
                        "No distributions for this sequence and window size! Now generating...");
                stmt.close();
                rs.close();
                throw new UnsupportedOperationException("Not supported yet.");
                /*stmt = this.conn.prepareStatement(
                        "SELECT seq_text FROM comp_dist_t " +
                        "WHERE cd_id = ? ");
                stmt.setInt(1, cdId);
                rs = stmt.executeQuery();
                if( rs.next() ) {
                    this.storeComposition(
                            this.generateComposition(rs.getString(1)));
                } else {
                    rs.close();
                    throw new SQLException("No sequence " +
                            "associated with CompositionDistribution '" +
                            cdId + "'!");
                }    
                 */             
            }
            rs.close();
        } catch (SQLException sqle ) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle ) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
    }
    
    /**
     * Constructor
     * 
     * @param windowSize
     * @param inSequence
     * @param cdId SQL Id of the parent CompositionDistributionSQL table
     * @param conn database connection
     */
    public CompositionDistributionSQLMap(Integer windowSize, String inSequence,
            Integer cdId, Connection conn) {
        this.windowSize = windowSize;
        this.conn = conn;
        this.cdId = cdId;
        
        //System.err.println("CompositionDistributionSQLMap: Generating map for sequence...");
        this.storeComposition(this.generateComposition(inSequence));
    }
    
     /**
     * Construct a new <tt>CompositionDistributionSQLMap</tt> object using a 
     * pre-generated map of compositions for this windowSize.
     * 
     * The nmer sizes are not checked!
     * 
     * @param windowSize
     * @param compMap compositions for this windowSize
     * @param cdId SQL Id of the parent CompositionDistributionSQL table
     * @param conn database connection
     */
    public CompositionDistributionSQLMap(Integer windowSize, Map<String, Integer> compMap,
            Integer cdId, Connection conn) {
        this.windowSize = windowSize;
        this.conn = conn;
        this.cdId = cdId;
        
        //System.err.println("CompositionDistributionSQLMap: Generating map for sequence...");
        this.storeComposition(compMap);
    }
    
    public boolean put(String str) {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put(str, 1);
        return putMany(map);
    }

    /**
     * Use this to update or put many counts at a time.
     * @param map of nmers and counts
     */
    public boolean putMany(Map<String, Integer> map) {
        PreparedStatement updateStmt = null;
        PreparedStatement insertStmt = null;
        Boolean suc = true;
        /**
         * TODO: redo for JDBC 2.0's new ResultSet where we can update
         * or insert the result set.
         * HashSet<String> uppedSet = new HashSet<String>();
         * if(map.has_key(rs.getString(?))) uppedSet.add(string); 
         * 
         * Added storeComposition for first time creation.
         */
        try {
            updateStmt = this.conn.prepareStatement(
                    "UPDATE comp_dist_map_t SET cnt = cnt + ? " +
                    "WHERE cd_id = ? AND nmer = ?" );
            insertStmt = this.conn.prepareStatement(insertCntStmt);

            insertStmt.setInt(1, this.cdId);
            insertStmt.setInt(2, this.windowSize);
            
            int n;
            for(Iterator<String> iter = map.keySet().iterator(); iter.hasNext(); ) {
                String nmer = iter.next();
                updateStmt.setInt(1, map.get(nmer));
                updateStmt.setInt(2, this.cdId);
                updateStmt.setString(3, nmer);
                n = updateStmt.executeUpdate();
                if (n == 0) {
                    //System.err.printf("CompositionDistributionSQLMap.putMany: inserting nmer '%s'\n", nmer);
                    /** Probably need to insert it. */
                    insertStmt.setString(3, nmer);
                    insertStmt.setInt(4, map.get(nmer));
                    insertStmt.executeUpdate();
                }
            }
        } catch (SQLException sqle ) {
            AbstractVectorSetSQL.printSQLException(sqle);
            suc = false;
        } finally {
            /** Release all open resources to avoid unnecessary memory usage. */
            try {                
                updateStmt.close();
                insertStmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        return suc;        
    }
    
    public void addMap(CompositionDistributionMap map) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Integer get(String str) {
        PreparedStatement selectStmt = null;
        Integer cnt = 0;
        try {
            selectStmt = this.conn.prepareStatement(
                    "SELECT cnt FROM comp_dist_map_t " +
                    "WHERE cd_id = ? AND window_size = ? " +
                    "AND nmer = ?" );
            selectStmt.setInt(1, this.cdId);
            selectStmt.setInt(2, this.windowSize);
            selectStmt.setString(3, str);
            
            ResultSet rs = selectStmt.executeQuery();
            if( rs.next() ) {
                cnt = rs.getInt(1);
            }
            rs.close();
        } catch (SQLException sqle ) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                selectStmt.close();
            } catch (SQLException sqle ) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        return cnt;
    }

    /**
     * Return an interator for all strings indexed.
     */
    public Iterator<String> iterator() {
        /**
         * DEBUG: It would be nice to pre-allocate the space for the TreeSet
         */
        TreeSet<String> set = new TreeSet<String>();
        PreparedStatement selectStmt = null;
        try {
            selectStmt = this.conn.prepareStatement(selectIteratorStmt);
            selectStmt.setInt(1, this.cdId);
            selectStmt.setInt(2, this.windowSize);
            ResultSet rs = selectStmt.executeQuery();
            while( rs.next() ) {
                set.add(rs.getString(1));
            }
            rs.close();
        } catch (SQLException sqle ) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                selectStmt.close();
            } catch (SQLException sqle ) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        //System.err.printf("CompositionDistributionSQLMAP.iterator: found %d nmers\n", set.size());
        return set.iterator();
    }

    /**
     * Returns the size (number) of strings we have counted.
     */
    public Integer size() {
        /** Should be count of nmers of this size */
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Returns the window size for this distribution map.
     */
    public Integer getWindowSize() {
        return this.windowSize;
    }

    public void addSequence(String inSequence) {
        this.putMany(this.generateComposition(inSequence));
    }
    
    /**
     * Generate the composition for this sequence
     * 
     * @param inSequence
     * @return
     */
    private HashMap<String, Integer> generateComposition(String inSequence) {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        
        for (int i = 0; i < inSequence.length() - this.windowSize + 1; ++i) {
            String subst = inSequence.substring(i, i + this.windowSize);
            if( map.containsKey(subst) ) {
                map.put(subst, map.get(subst) + 1);
            } else {
                map.put(subst, 1);
            }
        }
        return map;
    }
    
    /**
     * Store (insert) the composition using batch submit.
     * 
     * This will probably die if the nmer, window_size already exist.
     * 
     * @param map
     */
    private void storeComposition(Map<String, Integer> map) {
        PreparedStatement stmt = null;
         try {
            stmt = this.conn.prepareStatement(insertCntStmt);
            // cd_id, window_size, nmer, cnt
            stmt.setInt(1, this.cdId);
            stmt.setInt(2, this.windowSize);
            String nmer;
            for(Iterator<String> iter = map.keySet().iterator(); 
                    iter.hasNext(); ) {
                nmer = iter.next();
                stmt.setString(3, nmer);
                stmt.setInt(4, map.get(nmer));
                stmt.addBatch();
            }
            int [] updateCounts = stmt.executeBatch();
        } catch(BatchUpdateException b) {
            AbstractVectorSetSQL.printBatchUpdateException(b);
        } catch (SQLException sqle ) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle ) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
    }
}

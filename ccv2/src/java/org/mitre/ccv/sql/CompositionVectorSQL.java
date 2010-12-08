/**
 * CompositionVectorSQL.java
 *
 * Created on Jan 9, 2008, 10:52:33 AM
 *
 * $Id$
 */

package org.mitre.ccv.sql;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.mitre.ccv.AbstractCompositionVector;
import org.mitre.ccv.CompositionDistribution;
import org.mitre.ccv.CompositionVector;

/**
 *
 * @author Marc Colosimo
 */
public class CompositionVectorSQL extends AbstractCompositionVector {

    private CompositionDistribution cd;
    private Integer windowSize;
    private Connection conn;
    private Integer cdID;
    
    /** Insert generate pi values into the comp_vector_t */
    private final static String insertPiValuesStmt =
            "INSERT INTO comp_vector_t(cd_id, window_size, nmer, pi_value) " +
            "VALUES(?, ?, ?, ?)";
    
    /** Select the nmers and counts need to calculate our vector. */
    private final static String selectNmerStmt = 
            "SELECT nmer, cnt FROM " +
            "comp_dist_map_t WHERE " +
            "cd_id = ? AND window_size BETWEEN ? AND ? " +
            "ORDER BY nmer ";
    
    /**
     * Constructor: This uses the data in the compositionDistributionID 
     * entry to load a distribution to calculate the pi-values if they 
     * haven't been generated before.
     * 
     * @param windowSize
     * @param compositionDistributionID
     * @param conn
     
    public CompositionVectorSQL(Integer windowSize, 
            Integer compositionDistributionID, 
            Connection conn) {
        this(windowSize, null, compositionDistributionID, conn);
    }
     * */
    
    public CompositionVectorSQL(Integer windowSize, 
            CompositionDistribution cd, 
            Integer compositionDistributionID, 
            Connection conn) {
        this.cd = cd;
        this.windowSize = windowSize;
        this.conn = conn;
        this.cdID = compositionDistributionID;

        if( this.cd == null ) 
            this.cd = new CompositionDistributionSQL(this.cdID, this.conn);
        
        /** Check to see if we already stored the data */
        Boolean create = true;
        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT distinct(cd_id) FROM comp_vector_t " +
                    "WHERE cd_id = " + this.cdID + " " +
                    "AND window_size = " + this.windowSize);
            if( rs.next() ) {
                //System.err.println("CompositionVectorSQL: already have data.");
                create = false;
            }
            rs.close();
        }catch (SQLException sqle ) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle ) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        if( create ) 
            this.storeCompositionVector(this.createCompositionVector());
    }
    
    /**
     * Construct 
     * @param windowSize
     * @param cv CompositionVector to store
     * @param compositionDistributionID
     * @param conn
     */
    public CompositionVectorSQL(Integer windowSize, 
            CompositionVector cv,
            Integer compositionDistributionID, 
            Connection conn) {
        this.cd = cv.getCompositionDistribution();
        this.windowSize = windowSize;
        this.conn = conn;
        this.cdID = compositionDistributionID;
        
        /** Check to see if we already stored the data */
        Boolean create = true;
        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT distinct(cd_id) FROM comp_vector_t " +
                    "WHERE cd_id = " + this.cdID + " " +
                    "AND window_size = " + this.windowSize);
            if( rs.next() ) {
                System.err.println("CompositionVectorSQL: already have data.");
                create = false;
            }
            rs.close();
        }catch (SQLException sqle ) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle ) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        if( create ) 
            this.storeCompositionVector(cv.getCompositionVector());
    }
    
    public int getWindowSize() {
        return this.windowSize;
    }

    /**
     * Returns the pi-value for the nmer (tile).  
     */
    public Double getPiValueForNmer(String nmer) {
        Double value = null;
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(
                    "SELECT pi_value FROM comp_vector_t" +
                    "WHERE cd_id = ? AND nmer = ? ");
            stmt.setInt(1, this.cdID);
            stmt.setString(2, nmer);
            ResultSet rs = stmt.executeQuery();
            if( rs.next() ) {
                value = rs.getDouble(1);
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
        return value;
    }

    public Set<String> getNmers() {
        TreeSet<String> set = new TreeSet<String>();
        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT nmer FROM comp_vector_t" +
                    "WHERE cd_id = " + this.cdID + " ORDER BY nmer");
            while ( rs.next() ) {
                set.add(rs.getString(1));
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
        return set;
    }

    public CompositionDistribution getCompositionDistribution() {
       return this.cd;
    }

    /**
     * Returns the pi values for the composition vector.
     * 
     * This is might be a copy of the data or not. So <B>DO NOT</B> modify it.
     */
    public Map<String, Double> getCompositionVector() {
        Map<String, Double> map = new HashMap<String, Double>();
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(
                    "SELECT nmer, pi_value FROM comp_vector_t" +
                    "WHERE cd_id = ?");
            stmt.setInt(1, this.cdID);
            ResultSet rs = stmt.executeQuery();
            while( rs.next() ) {
                map.put(rs.getString(1), rs.getDouble(2));
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
        return map;
    }
    
    /**
     * Calculates the pi values for composition vector
     */
    @Override
    protected Map<String, Double> createCompositionVector() { 
        /** Get the counts for all of the nmers. */
        TreeMap<String, Integer> cnts = new TreeMap<String, Integer>();
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(selectNmerStmt);

            stmt.setInt(1, this.cdID);
            stmt.setInt(2, this.windowSize - 2);
            stmt.setInt(3, this.windowSize);
            ResultSet rs = stmt.executeQuery();
            while( rs.next() ) {
                cnts.put(rs.getString(1), rs.getInt(2));
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
        
        /** 
         * Need to calculate pi_values for strings between start and stop 
         */
        TreeMap<String, Double> compVector = new TreeMap<String, Double>();
        Integer totalSubStr = this.cd.getTotalSubStrings(this.windowSize);
        for(Iterator<String> iter = cnts.keySet().iterator();
                iter.hasNext(); ) {
            
            String nmer = iter.next();
            if( nmer.length() < this.windowSize )
                continue;

            //System.err.println(nmer);
            String s1 = nmer.substring(0, this.windowSize - 1);
            String s2 = nmer.substring(1, this.windowSize);
            String s3 = nmer.substring(1, this.windowSize - 1);

            compVector.put(nmer,
                    AbstractCompositionVector.calculatePiValue(
                    cnts.get(nmer), cnts.get(s1),
                    cnts.get(s2), cnts.get(s3),
                    totalSubStr));
        }
        return compVector;
    }
    
    /**
     * Store the calculated pi-values in the database.
     */
    private void storeCompositionVector(Map<String, Double> piValues) {
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(insertPiValuesStmt);
            stmt.setInt(1, this.cdID);
            stmt.setInt(2, this.windowSize);
            for(Iterator<Entry<String, Double>> 
                    iter = piValues.entrySet().iterator(); iter.hasNext(); ) {
                Entry<String, Double> entry = iter.next();
                stmt.setString(3,entry.getKey());
                stmt.setDouble(4,entry.getValue());
                stmt.addBatch();
            }
            int [] updateCounts = stmt.executeBatch();
        } catch(BatchUpdateException b) {
            System.err.println("----BatchUpdateException----");
            System.err.println("SQLState:  " + b.getSQLState());
            System.err.println("Message:  " + b.getMessage());
            System.err.println("Vendor:  " + b.getErrorCode());
            System.err.print("Update counts:  ");
            int [] updateCounts = b.getUpdateCounts();
            for (int i = 0; i < updateCounts.length; i++) {
                System.err.print(updateCounts[i] + "   ");
            }
            System.err.println("");
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

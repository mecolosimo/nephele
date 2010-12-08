/**
 * CompleteCompositionVectorSQL.java
 *
 * Created on Jan 3, 2008, 6:00:32 AM
 *
 * $Id
 */
package org.mitre.ccv.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.mitre.ccv.CompleteCompositionVector;
import org.mitre.ccv.CompositionDistribution;
import org.mitre.ccv.CompositionVector;
import weka.core.matrix.Matrix;

/**
 * A CompleteCompositionVector class backed by a SQL database.
 * 
 * @author Marc Colosimo
 */
public class CompleteCompositionVectorSQL
        implements CompleteCompositionVector {

    private Connection conn;
    /** Composition distribution table id. */
    private final Integer cdID;
    /** CompositionDistribution for this sequence. */
    private CompositionDistribution cd;
    /** List of our CompositionVectors. */
    private List<CompositionVector> cvs;
    /** Select statement for getting pi_value. */
    private final String getNmerValueStmt =
            "SELECT pi_value FROM comp_vector_t " +
            "WHERE cd_id = ? AND nmer = ? ";
    /** Select statement for getting all nmers. Redo with inner join to select */
    private final String getNmerSetStmt =
            "SELECT nmer FROM comp_dist_map_t " +
            "WHERE cd_id = ? AND " +
            "window_size BETWEEN ? AND ? " +
            "ORDER BY nmer ";

    /**
     * Construct a new <tt>CompleteCompositionVectorSQL</tt> object saving
     * it to a SQL database using another CompleteCompositionVector.
     * 
     * @param ccv
     * @param cd
     * @param conn
     */
   public CompleteCompositionVectorSQL(CompleteCompositionVector ccv, 
           CompositionDistributionSQL cd, Connection conn) {
        this.cdID = cd.getId();
        this.conn = conn;
        
        this.cvs = new LinkedList<CompositionVector>();
        
        /** DEBUG check the start and stops */
        Integer start = this.getStart();
        Integer stop = this.getStop();
        
        for (int i = start; i <= stop; i++) {
            this.cvs.add(
                    new CompositionVectorSQL(i, ccv.getCompositionVector(i),
                    this.cdID, this.conn));
        }
    }
    
   /**
    * Construct a new <tt>CompleteCompositionVectorSQL</tt> object saving
    * it to a SQL database using the given CompositionDistribution.
    * 
    * @param cd
    * @param compositionDistributionID
    * @param conn
    */
   public CompleteCompositionVectorSQL(CompositionDistribution cd, 
            Integer compositionDistributionID, Connection conn) {
        this.conn = conn;
        this.cdID = compositionDistributionID;

        /** DEBUG: CompositionDistributionSQL should load data if available */
        if( cd != null ) {
            this.cd = cd;
        } else {
            /** Try to load */
            System.err.println("CompleteCompositionVectorSQL: " +
                    "Loading CompositionDistribution... ");
            this.cd = new CompositionDistributionSQL(this.cdID, this.conn);
        }

        /** Load/Generate CompositionVectors */
        this.cvs = new LinkedList<CompositionVector>();
        Integer start = this.getStart();
        Integer stop = this.getStop();
        for (int i = start; i <= stop; i++) {
            this.cvs.add(
                    new CompositionVectorSQL(i, this.cd, this.cdID, this.conn));
        }
    }

    public String getName() {
        String name = "";
        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT seq_name FROM comp_dist_t WHERE cd_id = " + this.cdID);
            if (rs.next()) {
                name = rs.getString(1);
            }
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        return name;
    }
    
    /**
     * Returns a unqiue identifier for the sequence that this
     * <tt>CompleteCompositionVector</tt> represents.
     */
    public Integer getSequenceId() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public int getStart() {
        int start = 0;
        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
            // DEBUG: update query using JOIN ON
            ResultSet rs = stmt.executeQuery(
                    "SELECT start_window_size FROM comp_dist_t " +
                    "JOIN vector_set_t ON (comp_dist_t.vs_id = vector_set_t.vs_id) " +
                    "WHERE comp_dist_t.cd_id = " + this.cdID);
            if (rs.next()) {
                start = rs.getInt(1);
            }
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        return start;
    }

    public int getStop() {
        int stop = 0;
        Statement stmt = null;
        try {
            stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT stop_window_size FROM comp_dist_t " +
                    "JOIN vector_set_t ON (comp_dist_t.vs_id = vector_set_t.vs_id) " +
                    "WHERE comp_dist_t.cd_id = " + this.cdID);
            if (rs.next()) {
                stop = rs.getInt(1);
            }
            rs.close();
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        return stop;
    }

    public CompositionVector getCompositionVector(Integer windowSize) {
       return this.cvs.get(this.getStart()-windowSize);
    }

    public Matrix getMatrix(Set<String> nmers) {
        if (nmers == null || nmers.size() == 0) {
            return getMatrix();
        }

        Integer length = nmers.size();
        Matrix matrix = new Matrix(length, 1);
        PreparedStatement stmt = null;
 
        /** TODO: add call to get our size for the HashMap */
        HashMap<String, Double> cvt = new HashMap<String, Double>();
        try {
            stmt = this.conn.prepareStatement(
                    "SELECT nmer, pi_value FROM comp_vector_t " +
                    "WHERE cd_id = ? ");
            stmt.setInt(1, this.cdID);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                cvt.put(rs.getString(1), rs.getDouble(2));
            }
            rs.close();
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        Integer idx = 0;
        for (Iterator<String> iter = nmers.iterator(); iter.hasNext();) {
            String nmer = iter.next();
            if (cvt.containsKey(nmer)) {
                matrix.set(idx, 0, cvt.get(nmer));
            } else {
                matrix.set(idx, 0, 0.0);
            }
            idx++;
        }
        return matrix;
    }

    /**
     * Returns all the non-zero nmers over the range used to generate the 
     * complete composition vector
     * 
     * @return TreeSet of all of the nmers.
     */
    public TreeSet<String> getNmerSet() {
        TreeSet<String> nmers = new TreeSet<String>();

        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(this.getNmerSetStmt);
            stmt.setInt(1, this.cdID);
            stmt.setInt(2, this.getStart());
            stmt.setInt(3, this.getStop());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                nmers.add(rs.getString(1));
            }
            rs.close();
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        return nmers;
    }

    /**
     * Returns the pi value for the given nmer.
     */
    public Double getPiValueforNmer(String nmer) {
        Double value = null;
        PreparedStatement stmt = null;
        try {
            stmt = this.conn.prepareStatement(this.getNmerValueStmt);
            stmt.setInt(1, this.cdID);

            stmt.setString(2, nmer);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                value = rs.getDouble(1);
            }
            rs.close();
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        return value;
    }

    public CompositionDistribution getCompositionDistribution() {
        return this.cd;
    }

    /**
     * Returns a Matrix with all the nmer values.
     */
    private Matrix getMatrix() {
        System.err.println("CompleteCompsitionVectorSQL.getMatrix(): Untested code!");
        PreparedStatement stmt = null;

        /** add call to get our size for the HashMap */
        HashMap<String, Double> cvt = new HashMap<String, Double>();
        try {
            stmt = this.conn.prepareStatement(
                    "SELECT nmer, pi_value FROM comp_vector_t " +
                    "WHERE cd_id = ? ");
            stmt.setInt(1, this.cdID);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                cvt.put(rs.getString(1), rs.getDouble(2));
            } 
            rs.close();
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        Integer length = cvt.size();
        Matrix matrix = new Matrix(length, 1);
        Integer idx = 0;
        for (Iterator<String> iter = cvt.keySet().iterator(); iter.hasNext();) {
            String nmer = iter.next();
            matrix.set(idx, 0, cvt.get(nmer));
            idx++;
        }
        return matrix;
    }
}

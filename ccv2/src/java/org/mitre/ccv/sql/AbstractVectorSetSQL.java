/**
 * AbstractVectorSetSQL.java
 * 
 * Created on 7 Jan 2008
 * 
 * $Id$
 */
package org.mitre.ccv.sql;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.BatchUpdateException;
import java.util.List;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.math.linear.RealMatrix;
import org.mitre.ccv.AbstractCompositionVector;
import org.mitre.ccv.AbstractVectorSet;
import org.mitre.ccv.CompleteCompositionVector;

import org.mitre.ccv.CompleteMatrix;
import org.mitre.math.linear.RealMatrixUtils;
import weka.core.matrix.Matrix;

/**
 * Abstract class for a collection (list) of CompleteCompositionVectors
 * calculated over a given range of window sizes using a SQL database.
 * 
 * Implementing classes and databases need to support at a minimium 
 * SQL-92. 
 * 
 * @author Marc Colosimo
 */
public abstract class AbstractVectorSetSQL extends AbstractVectorSet {

    /**
     * Some of the code/sql statements are shared among methods. 
     * If you find a bug in one method, check others. 
     */
    
    /** 
     * Select all the non-zero nmers between our window sizes. 
     * This can create a very large file table because of the 'GROUP BY'.
     * 
     * Look into optimizing this statement.
     */
    private final static String getNmersStmt =
            "SELECT nmer FROM " +
            "comp_dist_t cdt JOIN comp_dist_map_t cdmt " +
            "ON (cdt.cd_id = cdmt.cd_id) " +
            "WHERE cdt.vs_id = ? " +  // AND cdmt.cnt > 0 if it is here, we have a count
            "AND cdmt.window_size BETWEEN ?  AND ? " +
            "GROUP BY nmer ";
    
    /** Select the sum of the counts for ALL the nmers. */
    private final static String getAllNmerCnts =
            "SELECT nmer, sum(cnt) FROM " +
            "comp_dist_t cdt JOIN comp_dist_map_t cdmt " +
            "ON (cdt.cd_id = cdmt.cd_id) " +
            "WHERE cdt.vs_id = ? GROUP BY nmer";

    /**
     * Returns the database connection.
     */
    abstract public Connection getConnection() ;
    
    /**
     * Returns the VectorSet ID (vs_id).
     */
    abstract public Integer getVectorSetId();
    
    /**
     * Finds the list of non-zero nmers.
     * @return java.util.TreeSet of nmers found in at least one sequence
     */
    public TreeSet<String> getNmers() {
        TreeSet<String> set = new TreeSet<String>();
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(getNmersStmt);
            stmt.setInt(1, this.getVectorSetId());
            stmt.setInt(2, this.getStart());
            stmt.setInt(3, this.getStop());

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                set.add(rs.getString(1));
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
        System.err.println("AbstractVectorSetSQL.getNmers(): Got " + set.size() + " nmers ");

        return set;
    }

    /**
     * Find the m top nmers with the highest entropy
     * (<code>null</code> or zero is allowed and will return all nmers).
     * 
     * @return java.util.TreeSet of top m nmers found
     */
    public TreeSet<String> getNmers(Integer m) {
        TreeSet<String> nmers = this.getNmers();
        if (m == null || m == 0) {
            return nmers;
        }
        System.err.println("AbstractVectorSetSQL.getNmers(int): Got " + nmers.size() + " nmers ");

        /** 
         * Generate the vector matrix for all sequences
         */
        //Matrix smtx;
        Map<Integer, Double> smtx;
        try {
            smtx = this.getSuperMatrix();
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return null;
        }
        
        CompleteMatrix cmtx = this.getFullMatrix(nmers);
        //Matrix mtx = cmtx.getMatrix();
        RealMatrix mtx = cmtx.getMatrix();
        TreeSet<EntropyPair> entSet = new TreeSet<EntropyPair>();
        ArrayList<String> nmersArray = new ArrayList<String>(nmers);
        
        int r = mtx.getRowDimension();
        int c = mtx.getColumnDimension();
        for (int i = 0; i < r; ++i) {
            double ent = 0;
            for (int j = 0; j < c; j++) {
                if (mtx.getEntry(i, j) != 0) {
                    ent += Math.abs(mtx.getEntry(i, j)) * Math.log(Math.abs(mtx.getEntry(i, j) /  smtx.get(i)));//smtx.get(i, 0)));
                }
            }
            ent = Math.abs(ent);
            entSet.add(new EntropyPair(nmersArray.get(i), new Double(ent)));
        }

        /** Create top nmer set */
        TreeSet<String> top = new TreeSet<String>();
        ArrayList<EntropyPair> entList = new ArrayList<EntropyPair>(entSet);

        if (m > entList.size()) {
            m = entList.size();
        }
        for (int j = 0; j < m; j++) {
            top.add(entList.get(j).getKey());
        }

        System.err.println("AbstractVectorSetSQL.getNmers(int): Found " + top.size() + " top nmers ");

        return top;
    }
        
    /**
     * Returns a Matrix of the non-zero nmers by sequence
     * (<code>null</code> or empty Set is allowed and returns a matrix with all the nmers).
     * 
     * @return weka.core.matrix.Matrix
     */
    public CompleteMatrix getFullMatrix(TreeSet<String> nmers) {
        if (nmers == null || nmers.isEmpty()) {
            nmers = this.getNmers();
        }
        Connection conn = this.getConnection();
        
        //Matrix wekaMatrix = new Matrix(nmers.size(),
         //       this.getVectors().size());
        RealMatrix matrix = RealMatrixUtils.getNewRealMatrix(nmers.size(),
          this.getVectors().size());
        
        PreparedStatement stmt = null;
        ArrayList<String> nmersArray = new ArrayList<String>(nmers);
        
        System.err.printf("AbstractVectorSetSQL.getFullMatrix: %d Vectors \n", this.getVectors().size());
        try {
            
            /** Assume that the CompleteCompositionVectors are loaded in order */
            Integer cdId = 0; 
            Integer curCdId = null;
            stmt = conn.prepareStatement(
                    "SELECT cvt.nmer, cvt.cd_id, cvt.pi_value FROM " +
                    "comp_vector_t cvt JOIN comp_dist_t cdt ON " +
                    "(cvt.cd_id = cdt.cd_id) " +
                    "WHERE cdt.vs_id = ? AND " +
                    "cvt.window_size BETWEEN ? AND ? " +
                    "ORDER BY cvt.cd_id ");
            stmt.setInt(1, this.getVectorSetId());
            stmt.setInt(2, this.getStart());
            stmt.setInt(3, this.getStop());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                if (curCdId == null) {
                    curCdId = rs.getInt(2);
                } else if ( curCdId != null && curCdId != rs.getInt(2)) {
                    cdId++;
                    curCdId = rs.getInt(2);
                }
                String nmer = rs.getString(1);
                if (nmers.contains(nmer)) {
                   //wekaMatrix.set(
                    matrix.setEntry(
                            nmersArray.indexOf(nmer),
                            cdId, rs.getDouble(3));
                }
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

        //return matrix;
        return new CompleteMatrix(this.getStart(), this.getStop(),
                new ArrayList<String>(nmers), 
                this.getSampleNames(), matrix); //wekaMatrix);
    }

    /**
     * Returns a Matrix of the non-zero nmers by sequence
     * (<code>null</code> or zero is allowed and this 
     * will return a matrix with all the nmers).
     * 
     * @return weka.core.matrix.Matrix
     */
    public CompleteMatrix getFullMatrix(Integer topNmers) {
        // Get a Runtime object
        //Runtime r = Runtime.getRuntime();
        //long availMem = r.freeMemory();
        System.err.println("AbstractVectorSetSQL.getFullMatrix(Integer): DO NOT USE!");
        TreeSet<String> set = this.getNmers(topNmers);
        
        // we end up calling getFullMatrix TWICE!!! FIX THIS!!!
        // getNmers calls getFullMatrix will all nmers!!!!
        return this.getFullMatrix(set);
    }
    
    /**
     * Returns a Matrix of the non-zero nmers
     * @param topNmers
     * @param entName file name to write out resulting entropies
     * @return
     */
    public CompleteMatrix getFullMatrix(Integer topNmers, String entName) {
        /**
         * If topNmers is null, just punt this after we get the nmers.
         */
        Matrix matrix = null;
        
        System.err.printf("%s.getFullMatrix(int, string): Entropy output needs to be implemented!\n",
                    this.getClass().getName());
        //return this.getFullMatrix(topNmers);
        Connection conn = this.getConnection();
        
        /** nmer set */
        TreeSet<String> set = new TreeSet<String>();

        String tmpNmersCntTable = "temp_vector_set_nmers_cnt_" 
                + this.getVectorSetId() + "_t";
        String tmpNmersPiTable = "temp_vector_set_nmers_pi_" 
                + this.getVectorSetId() + "_t";

        
        PreparedStatement stmt = null;
        try {
                  
            /** 
             * First generate the topNmers
             * 
             * Create a temp table of ALL nmer counts
             */
            String createStmt = "CREATE TEMPORARY TABLE " + 
                    tmpNmersCntTable +
                    " (" + getAllNmerCnts + " )";
            stmt = conn.prepareStatement(createStmt);
            stmt.setInt(1, this.getVectorSetId());
            //stmt.execute();
            
            /** Temp table for calulcated pi-values for superMatrix */
            createStmt = "CREATE TEMPORARY TABLE " + tmpNmersPiTable +
                    " (nmer VARCHAR(25) NOT NULL, " +
                    " window_size INT NOT NULL, " +
                    "pi_value DOUBLE NOT NULL ) ";
            stmt = conn.prepareStatement(createStmt);
            stmt.execute();
            
            String idxStmt = "CREATE INDEX " + tmpNmersPiTable + "_nmer_idx " +
                    "ON " + tmpNmersPiTable + " (nmer) ";
            stmt = conn.prepareStatement(idxStmt);
            stmt.execute();
           
            
            /** Calculate pi-values */
            Integer supLen = 0;
            List<CompleteCompositionVector> vectors = this.getVectors();
            for (int i = 0; i < vectors.size(); i++) {
                supLen += vectors.get(i).getCompositionDistribution().length();
            }
            
            /** 
             * Not easy to do on the SQL side.
             * Get our list of nmers
             */
            TreeMap<String, Integer> cnts = new TreeMap<String, Integer>();
            stmt = conn.prepareStatement(getAllNmerCnts);
            stmt.setInt(1, this.getVectorSetId());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                cnts.put(rs.getString(1), rs.getInt(2));
            }
            rs.close();
            
            System.err.printf("\t%s.getFullMatrix: Found %d nmers\n\t\t" +
                    "Calculating the super composition vector....\n",
                    this.getClass().getName(), cnts.size());
            
            /* Now populate the table, do we need an idx? */
            stmt = conn.prepareStatement(
                    "INSERT INTO " + tmpNmersPiTable + 
                    " (nmer, window_size, pi_value) " + 
                    "VALUES (?, ?, ?)");
            Integer curSize = this.getStart();
                    
            for (Entry<String, Integer> entry : cnts.entrySet()) {
                String nmer = entry.getKey();

                /* Skip -1 and -2 start size */
                if (nmer.length() < this.getStart()) {
                    continue;
                }

                /** 
                 * totalSubStr = L - k + 1
                 * where k = the windowSize and L is supLen
                 */
                curSize = nmer.length();
                Integer totalSubStr = supLen - curSize + 1;
                String s1 = nmer.substring(0, curSize - 1);
                String s2 = nmer.substring(1, curSize);
                String s3 = nmer.substring(1, curSize - 1);

                stmt.setString(1, nmer);
                stmt.setInt(2, nmer.length());
                stmt.setDouble(3, 
                        AbstractCompositionVector.calculatePiValue(
                        cnts.get(nmer), cnts.get(s1),
                        cnts.get(s2), cnts.get(s3),
                        totalSubStr));
                stmt.addBatch();
            }
            int [] updateCounts = stmt.executeBatch();
          
            int totalNmers = cnts.size();
            cnts.clear();
            cnts = null;
            
            System.err.printf("\t%s.getFullMatrix: Calculated super composition vector" +
                    "\n\t\tNow populating entropy table. Making mtx table....\n", 
                    this.getClass().getName());
            
            /** 
             * Now calculate the entropies 
             */
            String mtxTable = "mtx_" + this.getVectorSetId() + "_t";
            
            String mtxStmt = "SELECT cvt.nmer, cvt.pi_value FROM " +
                "comp_vector_t AS cvt JOIN comp_dist_t AS cdt ON " +
                "(cvt.cd_id = cdt.cd_id) " +
                "WHERE cdt.vs_id = ? AND cvt.window_size BETWEEN ? AND ? ";

         
            String mtxTableStmt = "CREATE TEMPORARY TABLE " + 
                    mtxTable + " AS " + mtxStmt;
            stmt = conn.prepareStatement(mtxTableStmt);
            stmt.setInt(1, this.getVectorSetId());
            stmt.setInt(2, this.getStart());
            stmt.setInt(3, this.getStop());
            stmt.execute();
           
            idxStmt = "CREATE INDEX " + mtxTable + "_nmer_idx " +
                    "ON " + mtxTable + " (nmer) ";
            stmt = conn.prepareStatement(idxStmt);
            stmt.execute();
            
            System.err.printf("\t%s.getFullMatrix: Made mtx table!\n" +
                    "\n\t\tGetting mtx/smtx pi values...\n", 
                    this.getClass().getName());
            

            BufferedWriter bw = null;
            if (entName != null && entName.length() != 0) {
                try {
                    bw = new BufferedWriter(new FileWriter(entName));
                } catch (Exception ioe) {
                    ioe.printStackTrace();
                }
            }
            
            /** 
             * Use either sql or in-memory entropy calculations.
             * If the user wants to find nmers by the Likelihood method.
             */
            if (topNmers != null && topNmers < 0)
                set = getTopNmersByEntropyMem(conn, topNmers,
                        tmpNmersPiTable, mtxTable, bw);
            else 
                set = getTopNmersByEntropySQL(conn, topNmers,
                        tmpNmersPiTable, mtxTable, bw);

            if (bw != null) {
                try {
                    bw.close();
                } catch (Exception ioe) {
                    ioe.printStackTrace();
                }
            }
            
            System.err.printf("\t%s.getFullMatrix: Found %d top nmers\n",
                    this.getClass().getName(), set.size());
            
            /** 
             * Drop temp tables
             * 
             * On MySQL these are dropped once the connect is closed, but
             * if we call this again then we'll get errors.
             */
            String dropStmt = "DROP TABLE " + tmpNmersPiTable;
            stmt = conn.prepareStatement(dropStmt);
            stmt.execute();
            
            dropStmt = "DROP TABLE " + mtxTable;
            stmt = conn.prepareStatement(dropStmt);
            stmt.execute();
            
        } catch (BatchUpdateException b) {
            AbstractVectorSetSQL.printBatchUpdateException(b);
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        //return matrix;
        return this.getFullMatrix(set);
    }

    /**
     * Prints details of an SQLException chain to <code>System.err</code>.
     * Details included are SQL State, Error code, Exception message.
     *
     * @param e the SQLException from which to print details.
     */
    public static void printSQLException(SQLException e) {
        /**
         * Unwraps the entire exception chain to unveil the real cause of the
         * SQLException.
         */
        while (e != null) {
            System.err.println("\n----- SQLException -----");
            System.err.println("  SQL State:  " + e.getSQLState());
            System.err.println("  Error Code: " + e.getErrorCode());
            System.err.println("  Message:    " + e.getMessage());
            System.err.println();
            e.printStackTrace(System.err);
            System.err.println();
            e = e.getNextException();
        }
    }

    public static void printBatchUpdateException(BatchUpdateException b) {
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
    }

    public CompleteMatrix getCompleteMatrix(Set<String> nmers) {
        /** TODO: getSuperMatrix and then remove all */
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    private CompleteMatrix getFullMatrix(Integer topNmers, PreparedStatement entStmt, 
            String entFile ) {
    
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     * Get the topNmers by their Entropy using SQL and in memory storage. 
     * 
     * @param conn
     * @param topNmers
     * @param tmpNmersPiTable
     * @param mtxTable
     * @param totalNmers
     * @return
     * @throws java.sql.SQLException
     */
    private TreeSet<String> getTopNmersByEntropyMem(Connection conn, Integer topNmers,
            String tmpNmersPiTable, String mtxTable, BufferedWriter bw) throws SQLException {

        /** Used for effient generation of collection spaces */
        //float loadFactor = (float) 0.75;
        TreeSet<String> set = new TreeSet<String>();

        String entStmt = "SELECT mtx.nmer, mtx.pi_value, smtx.pi_value " +
                "FROM " + tmpNmersPiTable + " AS smtx JOIN " +
                mtxTable + " AS mtx " +
                "ON (mtx.nmer = smtx.nmer) " +
                "ORDER BY mtx.nmer";

        PreparedStatement stmt = conn.prepareStatement(entStmt);
        ResultSet rs = stmt.executeQuery();

        System.err.printf("\t%s.getTopNmersByEntropyMem: Have mtx/smtx result!\n" +
                "\n\t\tNow populating entropy set...\n",
                this.getClass().getName());

        TreeSet<EntropyPair> entSet = new TreeSet<EntropyPair>();

        double ent = 0;
        String curNmer = null;
        int ncv = 0;
        while (rs.next()) {
            if (curNmer == null) {
                curNmer = rs.getString(1);
            }
            if (!curNmer.equals(rs.getString(1))) {
                /** Hit a new nmer block, save our current entropy pair */
                entSet.add(new EntropyPair(curNmer, Math.abs(ent)));
                curNmer = rs.getString(1);
                ent = 0;
                ncv++;
            }

            if (Double.isNaN(ent)) {
                System.err.printf("\t\tNaN before getting values for '%s'\n!", curNmer);
            }
            if (rs.getDouble(2) != 0) {
                ent += Math.abs(rs.getDouble(2)) *
                        Math.log(Math.abs(rs.getDouble(2) / rs.getDouble(3)));
            }
            if (Double.isNaN(ent)) {
                System.err.printf("\t\tNaN: for '%s', %f mtx, %f smtx\n",
                        curNmer, rs.getDouble(2), rs.getDouble(3));
            }
        }
        rs.close();

        System.err.printf("\t%s.getTopNmersByEntropyMem: have %d pairs\n", 
                this.getClass().getName(), entSet.size());

        /** 
         * If negative then we will use the Likelihood method to find the 
         * the best number of top nmers.
         */
        if (topNmers != null && topNmers < 0) {
            topNmers = this.getNmersByLikelihood(entSet, -1 * topNmers);
        }

        TreeSet<String> top = new TreeSet<String>();
        if (topNmers > entSet.size()) {
            topNmers = entSet.size();
        }
        int i = 0;
        for (EntropyPair ep : entSet) {

            if (ep.getValue() < 1 && topNmers == 0) {
                break;
            }
            if (i >= topNmers) {
                break;
            }
            top.add(ep.getKey());
            if (bw != null) {
                this.printEntropy(bw, rs.getString(1), rs.getDouble(2));
            }
            i++;
        }

        System.err.printf("\t%s.getTopNmersByEntropyMem: returning %d nmers \n", 
                this.getClass().getName(), top.size());
        return top;
    }
          
    /**
     * Get the TopNmer by using their entropy and all operations in SQL.
     * 
     * @param conn
     * @param topNmers
     * @param tmpNmersPiTable
     * @param mtxTable
     * @param bw
     * @return
     * @throws java.sql.SQLException
     */
    private TreeSet<String> getTopNmersByEntropySQL(Connection conn, Integer topNmers,
            String tmpNmersPiTable, String mtxTable, BufferedWriter bw) throws SQLException {

        TreeSet<String> set = new TreeSet<String>();

        String tmpNmersEntTable = "temp_vector_set_nmers_ent_" + this.getVectorSetId() + "_t";
        String createStmt = "CREATE TEMPORARY TABLE " +
                tmpNmersEntTable +
                " (nmer VARCHAR(25) NOT NULL, " +
                " ent DOUBLE NOT NULL ) ";

        PreparedStatement stmt = conn.prepareStatement(createStmt);
        stmt.execute();

        String entStmt = "SELECT mtx.nmer, mtx.pi_value, smtx.pi_value " +
                "FROM " + tmpNmersPiTable + " AS smtx JOIN " +
                mtxTable + " AS mtx " +
                "ON (mtx.nmer = smtx.nmer) " +
                "ORDER BY mtx.nmer";

        stmt = conn.prepareStatement(entStmt);
        ResultSet rs = stmt.executeQuery();

        System.err.printf("\t%s.getTopNmersByEntropySQL: Have mtx/smtx result!\n" +
                "\n\t\tNow populating entropy table...\n",
                this.getClass().getName());

        String insertStmt = "INSERT INTO " + tmpNmersEntTable + " (nmer, ent) " +
                "VALUES (?, ?)";
        stmt = conn.prepareStatement(insertStmt);

        double ent = 0;
        String curNmer = null;
        int ncv = 0;
        while (rs.next()) {
            if (curNmer == null) {
                curNmer = rs.getString(1);
            }
            if (!curNmer.equals(rs.getString(1))) {
                stmt.setString(1, curNmer);
                stmt.setDouble(2, Math.abs(ent));
                stmt.execute();
                curNmer = rs.getString(1);
                ent = 0;
                ncv++;
            }

            if (Double.isNaN(ent)) {
                System.err.printf("\tNaN before getting values for '%s'\n!", curNmer);
            }
            if (rs.getDouble(2) != 0) {
                ent += Math.abs(rs.getDouble(2)) *
                        Math.log(Math.abs(rs.getDouble(2) / rs.getDouble(3)));
            }
            if (Double.isNaN(ent)) {
                System.err.printf("\tNaN: for '%s', %f mtx, %f smtx\n",
                        curNmer, rs.getDouble(2), rs.getDouble(3));
            }
        }
        rs.close();
        
        System.err.printf("\t%s.getTopNmersByEntropySQL: Have entropy results for %d nmers!\n" +
                "\n\t\tNow getting topNmers...\n",
                this.getClass().getName(), ncv + 1);

        /** Now get the nmers */
        entStmt = "SELECT nmer, ent FROM " + tmpNmersEntTable;

        if (topNmers != null && topNmers == 0) {
            entStmt += " WHERE ent > 1.0 ";
        }

        /** Want highest to lowest */
        entStmt += " ORDER BY ent DESC"; 

        if (topNmers != null && topNmers > 0) {
            entStmt += " LIMIT ? ";
        }

        stmt = conn.prepareStatement(entStmt);
        if (topNmers != null && topNmers > 0) {
            stmt.setInt(1, topNmers);
        }

        rs = stmt.executeQuery();

        int cv = 0;
        while (rs.next()) {
            /** Is the entropy greater than our cut off? */
            if (topNmers != null && topNmers == 0 && rs.getDouble(2) < 1.0) {
                break;
            }

            set.add(rs.getString(1)); 
            if (bw != null) {
                this.printEntropy(bw, rs.getString(1), rs.getDouble(2));
            }
            cv++;
        }
        rs.close();
        
        String dropStmt = "DROP TABLE " + tmpNmersEntTable;
        stmt = conn.prepareStatement(dropStmt);
        stmt.execute();
        return set;
    }
    
    /**
     * Use the BufferedWriter to print out the nmer and entropy values
     * 
     * @param bw
     * @param nmer
     * @param ent
     */
    private void printEntropy(BufferedWriter bw, String nmer, Double ent) {
        try {         
            bw.write(nmer + "\t" + ent + "\n");
        } catch (Exception ioe) {
            ioe.printStackTrace();
        }
    }
    /**
     * Returns the vectors Matrix for all the sequences.
     * 
     * REDO save to temp table and join?
     */
    private Map<Integer, Double> getSuperMatrix() throws Exception {
        Integer supLen = 0;
        Connection conn = this.getConnection();
        List<CompleteCompositionVector> vectors = this.getVectors();
        for (int i = 0; i < vectors.size(); i++) {
            //supLen += vectors.get(i).getSequence().length();
            supLen += vectors.get(i).getCompositionDistribution().length();
        }

        /** Get the counts for all of the nmers. */
        TreeMap<String, Integer> cnts = new TreeMap<String, Integer>();
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(getAllNmerCnts);
            stmt.setInt(1, this.getVectorSetId());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                cnts.put(rs.getString(1), rs.getInt(2));
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

        //System.err.println("AbstractVectorSetSQL.getSuperMatrix: " +
        //cnts.size() + " nmers in composition distribution");
       
        /** 
         * Need to calculate pi_values for strings between start and stop 
         */
        TreeMap<String, Double> compVector = new TreeMap<String, Double>();
        Integer curSize = this.getStart();
        for (Iterator<Entry<String, Integer>> iter =
                cnts.entrySet().iterator();
                iter.hasNext();) {
            Entry<String, Integer> entry = iter.next();
            String nmer = entry.getKey();

            /* Skip -1 and -2 start size */
            if (nmer.length() < this.getStart()) {
                continue;
            }

            /** 
             * totalSubStr = L - k + 1
             * where k = the windowSize and L is supLen
             */
            curSize = nmer.length();
            Integer totalSubStr = supLen - curSize + 1;
            String s1 = nmer.substring(0, curSize - 1);
            String s2 = nmer.substring(1, curSize);
            String s3 = nmer.substring(1, curSize - 1);

            compVector.put(nmer,
                    AbstractCompositionVector.calculatePiValue(
                    cnts.get(nmer), cnts.get(s1),
                    cnts.get(s2), cnts.get(s3),
                    totalSubStr));
        }

        System.err.println("AbstractVectorSetSQL.getSuperMatrix: " +
                compVector.size() + " nmers in composition vector");
        /** Calculate Matrix */
        //Matrix matrix = new Matrix(compVector.size(), 1);
        float loadFactor = (float) 0.75;
        int initialCapacity = (int)((compVector.size() + 1) * (1/loadFactor));
        HashMap<Integer, Double> map = new HashMap<Integer, Double>(initialCapacity, loadFactor);
        Integer idx = 0;
        for (Iterator<String> iter =
                compVector.keySet().iterator();
                iter.hasNext();) {
            String nmer = iter.next();
            //matrix.set(idx, 0, compVector.get(nmer));
            map.put(idx, compVector.get(nmer));
            idx++;
        }
        return map; //matrix;
    } 
}

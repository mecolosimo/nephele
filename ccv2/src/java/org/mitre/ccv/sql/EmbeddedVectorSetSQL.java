/**
 * EmbeddedVectorSetSQL.java
 *
 * Created on Jan 16, 2008, 8:11:03 AM
 *
 * $Id$
 */
package org.mitre.ccv.sql;

import java.util.List;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.LinkedList;
import java.util.Properties;


import java.util.Set;
import org.mitre.ccv.CompleteCompositionVector;
import org.mitre.ccv.CompositionDistribution;
import org.mitre.ccv.index.IndexedCompleteCompositionVector;
import org.mitre.ccv.index.IndexedCompositionDistribution;
import weka.core.matrix.Matrix;

/**
 * Class for a collection (list) of CompleteCompositionVectors
 * calculated over a given range of window sizes. This uses an
 * embedded Derby Database to store the sequences and vectors. 
 * This creates the tables in the database.
 * 
 * The directory derbyDB will be created under the directory that
 * the system property derby.system.home points to, or the current
 * directory (user.dir) if derby.system.home is not set. In addition,
 * a log file (derby.log) will be generated. Both can be deleted 
 * after the run.
 * 
 * To remove the database, remove the directory derbyDB (the
 * same as the database name) and its contents.
 * 
 * org.mitre.bio.ccv.sql.fast=true will use more memory to build the 
 * ccvs but then store them in the database.
 * 
 * @author Marc Colosimo
 */
public class EmbeddedVectorSetSQL extends AbstractVectorSetSQL {
    
    /** 
     * Debry only supports SQL-92, so foreign keys are on tables not rows.
     * Debry does support autoincrement but the syntax is for identity columns:
     *      "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)"
     */
    /** The default framework is embedded. */
    private final String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    private final String protocol = "jdbc:derby:";
    /** The default database name. */
    private final static String dbName = "derbyDB";
    /** The connection handle. */
    private Connection conn = null;
    
    /**
     * Any changes to these tables should also be made to ccv_schema.sql!
     */
    
    /** Table for VectorSet data. */
    private final static String vectorSetStmt =
            "CREATE TABLE vector_set_t (" +
            "vs_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), " +
            "vs_name VARCHAR(255), " +
            "start_window_size INT NOT NULL," +
            "stop_window_size INT NOT NULL ) ";
    /** Table for CompositionDistribution data. */
    private final static String compositionDistributionStmt =
            "CREATE TABLE comp_dist_t (" +
            "cd_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), " +
            "vs_id INT, " + // CONSTRAINT vs_id_fk REFERENCES vector_set_t, " +
            "seq_name VARCHAR(225) NOT NULL )" +
            "seq_length INT NOT NULL )";
    /** Table for CompositionDistributionMap data. */
    private final static String compositionDistributionMapStmt =
            "CREATE TABLE comp_dist_map_t ( " +
            "cd_id INT, " + // CONSTRAINT cd_id_fk REFERENCES comp_dist_t, " +
            "window_size INT NOT NULL, " +
            "nmer VARCHAR(25) NOT NULL," +
            "cnt INT NOT NULL ) ";
    /** Table for SequenceCompositionVector data. */
    private final static String compostionVectorStmt =
            "CREATE TABLE comp_vector_t ( " +
            "cd_id INT NOT NULL, " + // CONSTRAINT cd_id_fk REFERENCES comp_dist_t, " +
            "window_size INT NOT NULL, " +
            "nmer VARCHAR(25) NOT NULL, " +
            "pi_value DOUBLE NOT NULL ) ";
    
    private Integer vsId;
    private Integer start;
    private Integer stop;
    private List<CompleteCompositionVector> vectors;

    /**
     * Construct a new <tt>EmbeddedVectorSetSQL</tt> object.
     * 
     * @param start
     * @param stop
     */
    public EmbeddedVectorSetSQL(Integer start, Integer stop) {
        this(start, stop, null);
    }

    /**
     * Construct a new <tt>EmbeddedVectorSetSQL</tt> object.
     *  
     * @param start
     * @param stop
     * @param props
     */
    public EmbeddedVectorSetSQL(Integer start, Integer stop, Properties props) {
        this.start = start;
        this.stop = stop;
        this.vectors = new LinkedList<CompleteCompositionVector>();

        this.loadDriver();
        Statement stmt = null;

        try {
            /** 
             * Connection properties. 
             */
            if (props == null) {
                props = new Properties();
            /**
             * A user name and password is optional in the embedded client
             */
            }

            /**
             * This connection specifies create=true in the connection URL to
             * cause the database to be created when connecting for the first
             * time. 
             */
            this.conn = DriverManager.getConnection(protocol + dbName + ";create=true", props);

            /**
             * We want to control transactions manually. 
             * Autocommit is on by default in JDBC
             
            this.conn.setAutoCommit(false);
            */
            
            /**
             * Creating a statement object that we can use for running various
             * SQL statements commands against the database.
             */
            stmt = conn.createStatement();

            /**
             * Create the VectorSet table (basically the data for this class).
             */
            stmt.execute(vectorSetStmt);

            /**
             * How do we check to see if we have created these tables before.
             */
            /** 
             * Create the CompositionDistribution tables.
             */
            stmt.execute(compositionDistributionStmt);

            stmt.execute(compositionDistributionMapStmt);
            stmt.execute("CREATE INDEX comp_dist_map_nmer_idx on comp_dist_map_t (nmer)");
            stmt.execute("CREATE INDEX comp_dist_map_window_size_idx on comp_dist_map_t (window_size)");
            /**
             * Create SequenceCompositionVector table.
             */
            stmt.execute(compostionVectorStmt);
            stmt.execute("CREATE INDEX comp_vector_nmer_idx on comp_vector_t (nmer)");
            stmt.execute("CREATE INDEX comp_vector_cd_idx on comp_vector_t (cd_id)");

            /** Insert our date into the table. */
            PreparedStatement psInsert = conn.prepareStatement(
                    "INSERT INTO vector_set_t(vs_name, start_window_size, stop_window_size) " +
                    "VALUES ('embeddedDerby', ?, ?)");
            psInsert.setInt(1, start);
            psInsert.setInt(2, stop);
            psInsert.executeUpdate();

            /** Get back our id. */
            ResultSet rs = stmt.executeQuery("SELECT MAX(vs_id) FROM vector_set_t");
            if (rs.next()) {
                this.vsId = rs.getInt(1);
            } else {
                throw new SQLException("No vs_id returned!");
            }
            rs.close();
        /** */
        /** How to handle shutting down smoothly? */
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            /** Release all open resources to avoid unnecessary memory usage. */
            try {
                stmt.close();
                System.err.println("EmbeddedVectorSet: Finished creating tables.");
            } catch (SQLException sqle) {
                printSQLException(sqle);
            }
        }
    }

    @Override
    public void addSequence(String seqName, String seq) {
        if (this.buildInMemory() ) {
            IndexedCompositionDistribution icd = new IndexedCompositionDistribution(
                    null, 1, seq, this.start, this.stop);
            CompleteCompositionVector ccv = new 
                    IndexedCompleteCompositionVector(seqName, 1, icd);
            CompositionDistributionSQL cds = new 
                    CompositionDistributionSQL(seqName, this.vsId, icd, this.conn);
            this.vectors.add(new CompleteCompositionVectorSQL(ccv, cds, conn));
        } else {
            CompositionDistributionSQL cd =
                    new CompositionDistributionSQL(seqName, seq, this.vsId, this.conn);
            CompleteCompositionVector ccv =
                    new CompleteCompositionVectorSQL(cd, cd.getId(), this.conn);
            this.vectors.add(ccv);
        }
    }

    /**
     * Returns the list of CompleteCompositionVectors
     */
    @Override
    public List<CompleteCompositionVector> getVectors() {
        // DEBUG: add support to query the db for these.
        return this.vectors;
    }

    @Override
    public Integer getStart() {
        return this.start;
    }

    @Override
    public Integer getStop() {
        return this.stop;
    }
    
    /**
     * Returns our database connection.
     */
    @Override
    public Connection getConnection() {
        return this.conn;
    }

    /**
     * Returns the VectorSet table Id (vs_id).
     */
    @Override
    public Integer getVectorSetId() {
        return this.vsId;
    }

    /**
     * Return true if System property is set to be fast.
     */
    private boolean buildInMemory() {
       if (System.getProperty("org.mitre.bio.ccv.sql.fast") != null &&
           System.getProperty("org.mitre.bio.ccv.sql.fast").equalsIgnoreCase("true"))
       {    System.err.println("EmbeddedVectorSetSQL: using fast memory");
           return true;
       } else
           return false;
    }
    
    /**
     * LoadsDerby's embedded Driver, <code>org.apache.derby.jdbc.EmbeddedDriver</code>.
     */
    private void loadDriver() {
        try {
            Class.forName(driver).newInstance();
            System.err.printf("EmbeddedVectorSet: Loaded the appropriate driver (%s)\n", driver);
        } catch (ClassNotFoundException cnfe) {
            System.err.printf("\nEmbeddedVectorSet:Unable to load the JDBC driver (%s)\n", driver);
            System.err.println("Please check your CLASSPATH.");
            cnfe.printStackTrace(System.err);
        } catch (InstantiationException ie) {
            System.err.printf(
                    "\nEmbeddedVectorSet:Unable to instantiate the JDBC driver (%s)", driver);
            ie.printStackTrace(System.err);
        } catch (IllegalAccessException iae) {
            System.err.println(
                    "\nNot allowed to access the JDBC driver " + driver);
            iae.printStackTrace(System.err);
        }
    }
}

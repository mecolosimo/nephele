/**
 * VectorSetSQL.java
 *
 * Created on Jan 16, 2008, 11:13:06 AM
 *
 * $Id$
 */
package org.mitre.ccv.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.mitre.ccv.CompleteCompositionVector;

/**
 * Class for a collection (list) of CompleteCompositionVectors
 * calculated over a given range of window sizes using a SQL database.
 * 
 * Connecting databases need to support at a minimium SQL-92.  
 * 
 * This will not create the tables required by this class and the 
 * classes that it uses. 
 * 
 * @author Marc Colosimo
 */
public class VectorSetSQL extends AbstractVectorSetSQL {

    private Connection conn;
    
    private Integer vsId;
    
    private String vsName = null;
    
    private Integer start = null;
    
    private Integer stop = null;
    
    private List<CompleteCompositionVector> vectors;
    
    /** Insert start, stop into vector_set_t */
    private final static String insertVstStmt = 
            "INSERT INTO vector_set_t" +
            "(vs_name, start_window_size, stop_window_size) " +
            "VALUES (?, ?, ?)";
    
    /**
     * Connect to a database using the given connection. 
     * <P>
     * This will create a new entry in the vector_set_t
     * using the next available vs_id.
     * 
     * @param conn the database connection.
     */
    public VectorSetSQL(Connection conn, Integer start, Integer stop) {
       this.conn = conn;
       this.start = start;
       this.stop = stop;
       this.vectors = new LinkedList<CompleteCompositionVector>();
 
       this.insertIntoTable();
    }
    
    /**
     * Connect to a database using the given connection and
     * vsId. 
     * <P>
     * If the vector_set_t has an entry with the vs_id, then it 
     * will use that data and add to it. If it does not exist,
     * a SQLException will be thrown. 
     * 
     * @param conn the database connection.
     * @param vsId the vs_id to look up.
     */
    public VectorSetSQL(Connection conn, Integer vsId)
            throws SQLException {
        
        this.conn = conn;
        this.vsId = vsId;
        
        this.vectors = new LinkedList<CompleteCompositionVector>();
        
        this.loadFromTable();
    }
    
    /**
     * Connect to a database using the given properties.
     * <P>
     * Expected properties:
     * ccv.jdbc.driver      - com.mysql.jdbc.Driver
     * ccv.jdbc.url         - jdbc:mysql://localhost/dbName
     * ccv.jdbc.username    - test
     * ccv.jdbc.password    - test
     * <BR>
     * Optional properties:
     * ccv.jdbc.vsId        - vs_id for table to load from if it exists.
     * ccv.jdbc.vsName      - name for vector set (if creating one).
     * ccv.start            - starting window size, if not given as arg
     * ccv.stop             - ending window size, if not given as arg
     * 
     * @param start <code>null</code> allowed if in properties
     * @param stop <code>null</code> allowed if in properties
     * @param props
     * @throws java.sql.SQLException
     */
    public VectorSetSQL(Integer start, Integer stop, Properties props) 
            throws SQLException {
        
        if( start == null )
            start = Integer.valueOf(props.getProperty("ccv.start"));
        this.start = start;
        if( stop == null )
            stop = Integer.valueOf(props.getProperty("ccv.stop"));
        this.stop = stop;
        
        this.vectors = new LinkedList<CompleteCompositionVector>();
        
        this.conn = this.getConnection(props);
        if( props.containsKey("ccv.jdbc.vsId") ) {
            this.vsId = Integer.parseInt(props.getProperty("ccv.jdbc.vsId").trim());
            this.loadFromTable();
        } else {
            this.vsName = props.getProperty("ccv.jdbc.vsName");
            this.insertIntoTable();
        }
    }
    
    @Override
    public Connection getConnection() {
        return this.conn;
    }

    @Override
    public Integer getVectorSetId() {
        return this.vsId;
    }

    @Override
    public void addSequence(String seqName, String seq) {
        CompositionDistributionSQL cd =
                new CompositionDistributionSQL(seqName, seq, this.vsId, this.conn);
        CompleteCompositionVector ccv =
                new CompleteCompositionVectorSQL(cd, cd.getId(), this.conn);
        this.vectors.add(ccv);
    }

    @Override
    public List<CompleteCompositionVector> getVectors() {
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

    private void insertIntoTable() {
       PreparedStatement stmt = null;
       try {
            /** Insert our data into the table. */
            stmt = this.conn.prepareStatement(insertVstStmt);
            stmt.setString(1, this.vsName);
            stmt.setInt(2, this.start);
            stmt.setInt(3, this.stop);
            stmt.executeUpdate();  // returns the number of rows 
                        
            /** Get back our id. */
            ResultSet rs = stmt.executeQuery(
                    "SELECT MAX(vs_id) FROM vector_set_t");
            if (rs.next()) {
                this.vsId = rs.getInt(1);
            } else {
                throw new SQLException("No vs_id returned!");
            }
            rs.close();
        } catch (SQLException sqle) {
            printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                printSQLException(sqle);
            }
        }
    }
    
    /**
     * Loads our data from the database.
     * <P>
     * If there is no matching vs_id then a SQLException is thrown.
     * 
     * If the given start/stop window sizes do not match, 
     * then a SQLException is thrown (start/stop are not needed and
     * can be loaded from the table).
     * 
     * @throws java.sql.SQLException
     */
    private void loadFromTable() 
            throws SQLException {
       PreparedStatement stmt = null;
       try {
            /** select our data */
            stmt = this.conn.prepareStatement(
                    "SELECT vs_name, start_window_size, stop_window_size " +
                    "FROM vector_set_t " +
                    "WHERE vs_id = ? ");
            stmt.setInt(1,this.vsId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                this.vsName = rs.getString(1);
                if( this.start != null && this.start != rs.getInt(2) )
                    throw new SQLException("Returned start '" +
                            + rs.getInt(2) + "' does not equal the given " +
                            "start '" + this.start + "'!");
                this.start = rs.getInt(2);
                if( this.stop != null && this.stop != rs.getInt(3) )
                    throw new SQLException("Returned stop '" +
                            + rs.getInt(3) + "' does not equal the given " +
                            "stop '" + this.stop + "'!");
                this.stop = rs.getInt(3);
                System.err.println("VectorSetSQL.loadFromTable: " +
                        "Loaded entry with correct fields in the table!");
            } else {
                throw new SQLException("No entry with vs_id " 
                        + this.vsId + " returned!");
            }
            rs.close();
            stmt.close();
            
            /** Now load our CompleteCompositionVectors */
            stmt = this.conn.prepareStatement(
                    "SELECT cd_id FROM comp_dist_t " +
                    "WHERE vs_id = ? ORDER by cd_id ");
            stmt.setInt(1, this.vsId);
            rs = stmt.executeQuery();
            while( rs.next() ) {
                Integer cdId = rs.getInt(1);
                CompleteCompositionVector ccv = 
                        new CompleteCompositionVectorSQL(null, cdId, this.conn);
                this.vectors.add(ccv);
            }
        } catch (SQLException sqle) {
            printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                printSQLException(sqle);
            }
        }
    }
    
    /**
     * Loads the driver and gets a connection using the 
     * information supplied in the properties.
     *
     * @return java.sql.Connection
     */
    private Connection getConnection(Properties props) 
            throws SQLException {
        
        String driver = props.getProperty("ccv.jdbc.driver");
        if( driver == null ) {
            System.err.println("VectorSetSQL: No JDBC driver property found!");
            return null;
        }
        try {
            Class.forName(driver).newInstance();
            System.err.printf("VectorSet: Loaded the appropriate driver (%s)\n", driver);
        } catch (ClassNotFoundException cnfe) {
            System.err.printf("VectorSet:Unable to load the JDBC driver (%s)\n", driver);
            System.err.println("Please check your CLASSPATH.");
            cnfe.printStackTrace(System.err);
        } catch (InstantiationException ie) {
            System.err.printf(
                    "VectorSet:Unable to instantiate the JDBC driver (%s)\n", driver);
            ie.printStackTrace(System.err);
        } catch (IllegalAccessException iae) {
            System.err.println(
                    "Not allowed to access the JDBC driver " + driver);
            iae.printStackTrace(System.err);
        }
        String url = props.getProperty("ccv.jdbc.url");
        String usr = props.getProperty("ccv.jdbc.username");
        String pwd = props.getProperty("ccv.jdbc.password");
        System.err.printf("VectorSetSQL: Trying to connect to %s;%s;%s\n", url, usr, pwd);
        return DriverManager.getConnection(url, usr, pwd);
    }
}

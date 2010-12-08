/**
 * CompositionCounts.java
 *
 * Created on May 29, 2008, 4:27:00 PM
 *
 * $Id$
 */

package org.mitre.ccv.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Class backed by a temporary SQL table for nmer counts.
 * 
 * @author Marc Colosimo
 */
public class CompositionCounts {

    public Boolean created = false;
    public final String tableName;
    public final Integer vsId;
    public final Connection conn;
    
    /** 
     * Select all the non-zero nmers between our window sizes. 
     * This can create a very large file table because of the 'GROUP BY'.
     * 
     * Look into optimizing this statement.
     */
    private final static String getNmersStmt =
            "SELECT nmer FROM ? " +
            "WHERE cnt > 0 " +
            "AND cdmt.window_size BETWEEN ?  AND ? " +
            "GROUP BY nmer ";
    
    /** Select the sum of the counts for ALL the nmers. */
    private final static String getAllNmerCnts =
            "SELECT nmer, sum(cnt) AS cnt FROM " +
            "comp_dist_t cdt JOIN comp_dist_map_t cdmt " +
            "ON (cdt.cd_id = cdmt.cd_id) " +
            "WHERE cdt.vs_id = ? GROUP BY nmer";

    public CompositionCounts(Integer vsId, Connection conn) throws Exception {
        this.tableName = "nmers_count_table_" + vsId;
        this.conn = conn;
        this.vsId = vsId;
        this.initialize();
        if (!this.created) {
            throw new Exception("TEMPERARY TABLE NOT CREATED!");
        }
    }

    /**
     * Clear the data
     */
    public void clear() {
        if (! this.created )
            return;
        
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement("DROP TABLE " + this.tableName);
            stmt.execute();
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
        this.created = false;
    }

    
    /**
     * Create table 
     */
    private void initialize() {
        String createStmt = "CREATE TEMPORARY TABLE " + this.tableName +
                " (" + getAllNmerCnts + ")";

        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(createStmt);
            stmt.setInt(1, this.vsId);
            stmt.execute();
            created = true;
        } catch (SQLException sqle) {
            AbstractVectorSetSQL.printSQLException(sqle);
        } finally {
            try {
                stmt.close();
            } catch (SQLException sqle) {
                AbstractVectorSetSQL.printSQLException(sqle);
            }
        }
    }
    
    /**
     * 
     * 
     * @throws java.lang.Throwable
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            clear();        // close open files
        } finally {
            super.finalize();
        }
    }
}

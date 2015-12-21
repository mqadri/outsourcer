import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by qadrim on 15-12-18.
 */
public class Netezza {
    private static String myclass = "Netezza";
    public static boolean debug = false;

    private static String getSQLForColumns(String sourceSchema, String sourceTable) throws SQLException
    {
        String method = "getSQLForColumns";
        int location = 1000;

        try
        {
            location = 2000;
            String strSQL = "select ATTNAME  AS COLUMN_NAME\n" +
                    "FROM _V_RELATION_COLUMN \n" +
                    "WHERE OWNER = '" + sourceSchema + "' AND NAME = '" + sourceTable + "' AND FORMAT_TYPE NOT IN ('BLOB')\n" +
                    "ORDER BY ATTNUM;\n";
            if (debug)
                Logger.printMsg("Returning: " + strSQL);

            location = 2100;
            return strSQL;
        }
        catch (Exception ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }
    }

    public static String getSQLForData(Connection conn, String sourceSchema, String sourceTable, String refreshType, String appendColumnName, int appendColumnMax) throws SQLException
    {
        String method = "getSQLForData";
        int location = 1000;

        try
        {
            location = 2000;
            //Create SQL Statement for getting the column names
            String strSQL = getSQLForColumns(sourceSchema, sourceTable);

            location = 2100;
            //Execute SQL Statement and format columns for next SELECT statement
            String columnSQL = CommonDB.formatSQLForColumnName(conn, strSQL);

            location = 2200;
            //Create SQL Statement for retrieving data from table
            strSQL = "SELECT " + columnSQL + " \n" +
                    "FROM \"" + sourceSchema + "\".\"" + sourceTable + "\" \n";

            location = 2300;
            //Add filter for append refreshType
            if (debug)
                Logger.printMsg("About to refreshType: " + refreshType);

            if (refreshType.equals("append"))
            {
                location = 2400;
                strSQL = strSQL + "WHERE \"" + appendColumnName + "\" > " + appendColumnMax;  //greater than what is in GP currently
            }

            if (debug)
                Logger.printMsg("Returning: " + strSQL);

            location = 2500;
            return strSQL;
        }
        catch (SQLException ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }
    }


    public static String getSQLForCreateTable(Connection conn, String sourceSchema, String sourceTable) throws SQLException
    {
        String method = "getSQLForCreateTable";
        int location = 1000;

        try
        {
            location = 2000;
            String strSQL = "SELECT  X.ATTNAME AS COLUMN_NAME,\n" +
                    "CASE \tWHEN Y.DATATYPE = 'BPCHAR' THEN 'character'\n" +
                    "\t\tWHEN Y.DATATYPE = 'NCHAR' THEN 'character'\n" +
                    "\t\tWHEN Y.DATATYPE = 'VARCHAR' AND X.ATTCOLLENG >= 20 THEN ' text '\n " +
                    "\t\tWHEN Y.DATATYPE = 'VARCHAR' AND X.ATTCOLLENG < 20 THEN 'character'\n" +
                    "\t\tWHEN Y.DATATYPE = 'NVARCHAR' THEN 'character'\n" +
                    "\t  \tWHEN Y.DATATYPE = 'BOOL' THEN 'bool'\n" +
                    "\t  \tWHEN Y.DATATYPE = 'INT1' THEN 'smallint'\n" +
                    "\t  \tWHEN Y.DATATYPE = 'NUMERIC' THEN X.FORMAT_TYPE\n" +
                    "\t  \tWHEN Y.DATATYPE = 'VARBINARY' THEN 'bytea'\n" +
                    "\t  \tELSE Y.DATATYPE  END ||   -- NO CONVERSION REQ: INT8, INT4, INT2, DATE, FLOAT8, FLOAT4, INTERVAL, NUMERIC\n" +
                    "CASE WHEN (Y.DATATYPE IN ('BPCHAR','NCHAR','NVARCHAR') or (Y.DATATYPE = 'VARCHAR' AND X.ATTCOLLENG < 20)) THEN '(' || X.ATTCOLLENG+2 || ') ' ELSE ' ' END AS DATATYPE, \n" +
                    //"Y.DATATYPE,\n" +
                    "X.ATTCOLLENG AS DATA_LENGTH, X.FORMAT_TYPE \n" +
                    " FROM \t\"_V_RELATION_COLUMN\" AS X , _V_datatype as Y\n" +
                    "WHERE X.OWNER = '" + sourceSchema + "' AND X.NAME = '" + sourceTable + "' AND X.ATTTYPID = Y.OBJID AND Y.DATATYPE NOT IN ('ST_GEOMETRY')\n" +
                    "ORDER BY ATTNUM";

            if (debug)
                Logger.printMsg("Returning: " + strSQL);

            location = 2100;
            return strSQL;

        }
        catch (Exception ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }
    }

    public static String getSQLForDistribution(Connection conn, String sourceSchema, String sourceTable) throws SQLException
    {
        String method = "getSQLForDistribution";
        int location = 1000;

        try
        {
            location = 2000;
            String strSQL = "select '\"' || ATTNAME || '\"' as COLUMN_NAME \n" +
                    "from _V_TABLE_DIST_MAP\n" +
                    "WHERE OWNER = '" + sourceSchema + "' AND TABLENAME = '" + sourceTable + "' ORDER BY DISTSEQNO;";


            if (debug)
                Logger.printMsg("Returning: " + strSQL);

            location = 2100;
            return strSQL;

        }
        catch (Exception ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }
    }
    public static String validate(Connection conn) throws SQLException
    {
        String method = "validate";
        int location = 1000;

        try
        {
            location = 2000;
            String msg = "";
            String strSQL = "select system_software_version from _v_system_info;\n";

            location = 2100;
            Statement stmt = conn.createStatement();

            location = 2200;
            ResultSet rs = stmt.executeQuery(strSQL);

            location = 2300;
            while (rs.next())
            {
                msg = "Success!";
            }

            location = 2400;
            return msg;
        }

        catch (SQLException ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }

    }
    public static int getMaxId(Connection conn, String sourceSchema, String sourceTable, String columnName) throws SQLException
    {
        String method = "getMaxId";
        int location = 1000;

        try
        {
            location = 2000;
            int maxId = -1;
            String strSQL = "SELECT MAX(\"" + columnName + "\") \n" +
                    "FROM \"" + sourceSchema + "\".\"" + sourceTable + "\"";

            location = 2100;
            Statement stmt = conn.createStatement();

            location = 2200;
            ResultSet rs = stmt.executeQuery(strSQL);

            location = 2300;
            while (rs.next())
            {
                maxId = rs.getInt(1);
            }

            location = 2400;
            return maxId;
        }

        catch (SQLException ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }

    }
    public static void checkSourceSchema(Connection conn, String sourceDatabase, String sourceSchema) throws SQLException
    {
        String method = "checkSourceSchema";
        int location = 1000;

        try
        {
            location = 2000;
            boolean found = false;

            location = 2100;
            Statement stmt = conn.createStatement();

            location = 2200;
            String strSQL = "select DISTINCT(OWNER)\n" +
                    "from _V_OBJECTS \n" +
                    "where OWNER = '" + sourceSchema + "'";


            if (debug)
                Logger.printMsg("Executing SQL: " + strSQL);

            location = 2300;
            ResultSet rs = stmt.executeQuery(strSQL);

            location = 2400;
            while (rs.next())
            {
                location = 2500;
                found = true;
            }

            if (!(found))
            {
                throw new SQLException("SourceSchema: \"" + sourceSchema + "\" NOT FOUND!");
            }
        }

        catch (SQLException ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }
    }
    public static ResultSet getSchemaList(Connection conn) throws SQLException
    {
        String method = "getSchemaList";
        int location = 1000;

        try
        {
            location = 2000;
            //only get schemas in which there is a table or view
            String strSQL = "select DISTINCT(OWNER)\n" +
                    "from _V_OBJECTS \n" +
                    "where OWNER NOT IN ('SYSTEM');\n";

            location = 2100;
            Statement stmt = conn.createStatement();

            location = 2200;
            ResultSet rs = stmt.executeQuery(strSQL);

            location = 3000;
            return rs;
        }
        catch (SQLException ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }
    }
    public static ResultSet getTableList(Connection conn, String sourceSchema) throws SQLException
    {
        String method = "getTableList";
        int location = 1000;

        try
        {
            location = 2000;
            //only get tables in which there is a table or view
            String strSQL = "select OBJNAME from _V_OBJECTS WHERE OWNER = '" + sourceSchema + "'";

            location = 2100;
            Statement stmt = conn.createStatement();

            location = 2200;
            ResultSet rs = stmt.executeQuery(strSQL);

            location = 3000;
            return rs;
        }
        catch (SQLException ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }
    }
    public static void checkSourceTable(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable) throws SQLException
    {
        String method = "checkSourceTable";
        int location = 1000;

        try
        {
            location = 2000;
            boolean found = false;

            location = 2100;
            Statement stmt = conn.createStatement();

            location = 2200;
            String strSQL = "select OBJNAME from _V_OBJECTS\n" +
                    "WHERE OWNER = '" + sourceSchema + "' AND OBJNAME = '"+ sourceTable + "'";

            if (debug)
                Logger.printMsg("Executing SQL: " + strSQL);

            location = 2300;
            ResultSet rs = stmt.executeQuery(strSQL);

            location = 2400;
            while (rs.next())
            {
                location = 2500;
                found = true;
            }

            if (!(found))
            {
                throw new SQLException("SourceTable: \"" + sourceSchema + "\".\"" + sourceTable + "\" NOT FOUND!");
            }
        }

        catch (SQLException ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }
    }
    public static void checkAppendColumnName(Connection conn, String sourceDatabase, String sourceSchema, String sourceTable, String appendColumnName) throws SQLException
    {
        String method = "checkAppendColumnName";
        int location = 1000;

        try
        {
            location = 2000;
            boolean found = false;

            location = 2100;
            Statement stmt = conn.createStatement();

            location = 2200;
            String strSQL = "SELECT NULL FROM _V_RELATION_COLUMN\n" +
                    " WHERE OWNER = '" + sourceSchema + "' AND NAME ='" + sourceTable + "' AND ATTNAME = '" + appendColumnName + "';\n";

            if (debug)
                Logger.printMsg("Executing SQL: " + strSQL);

            location = 2300;
            ResultSet rs = stmt.executeQuery(strSQL);

            location = 2400;
            while (rs.next())
            {
                location = 2500;
                found = true;
            }

            if (!(found))
            {
                throw new SQLException("AppendColumnName: \"" + appendColumnName + "\" does not exist in the SourceTable: \"" + sourceSchema + "\".\"" + sourceTable + "\"!");
            }
        }

        catch (SQLException ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }
    }

    public static void createExternalTable(Connection conn, String sourceSchema, String refreshType, String sourceTable, String targetSchema, String targetTable, int maxId, int queueId,
                                           String sourceServer, String sourceDatabase, int sourcePort, String sourceUser, String sourcePass) throws SQLException
    {
        String method = "createExternalTable";
        int location = 1000;

        try
        {
            location = 2000;
            String externalTable = GP.getExternalTableName(targetSchema, targetTable);

            location = 2100;
            Statement stmt = conn.createStatement();

            String createSQL = "CREATE EXTERNAL WEB TABLE \"" + GP.externalSchema + "\".\"" + externalTable + "\" \n (";

            location = 2309;
            String strSQL = "SELECT c.column_name, \n" +
                    "       CASE WHEN c.data_type = 'character' THEN c.data_type || '(' || c.character_maximum_length || ')' ELSE c.data_type END AS data_type \n" +
                    "FROM INFORMATION_SCHEMA.COLUMNS c \n" +
                    "WHERE table_schema = '" + targetSchema + "' \n" +
                    "       AND table_name = '" + targetTable + "' \n" +
                    "ORDER BY ordinal_position";

            location = 2400;
            ResultSet rs = stmt.executeQuery(strSQL);

            String nzSQL = "SELECT ";

            location = 2500;
            while (rs.next())
            {
                location = 2600;
                if (rs.getRow() == 1)
                {
                    location = 2700;
                    createSQL = createSQL + "\"" + rs.getString(1) + "\" " + rs.getString(2);
                    nzSQL = nzSQL + "\\'~\\' || " + rs.getString(1) + " || \\'~\\'";
                }
                else
                {
                    location = 2800;
                    createSQL = createSQL + ", \n \"" + rs.getString(1) + "\" " + rs.getString(2);
                    nzSQL = nzSQL + ", \\'~\\' || " + rs.getString(1) + " || \\'~\\'";
                }
            }

            nzSQL += " FROM ";
            // Use Select * for now
            nzSQL = "SELECT * FROM ";
            location = 2900;
            createSQL = createSQL + ") \n";

            ////////////////////////////////////////////
            //Create location for External Table
            ////////////////////////////////////////////
            location = 3000;
            //sourceTable = sourceTable.replaceAll("\\$", "\\\\\\\\\\$");
            //need to double check this

            location = 3100;
            /*String extLocation =    "LOCATION ('gpfdist://" + osServer + ":" + jobPort +
                    "/config.properties+" + queueId + "+" + maxId + "+" + refreshType + "+" + sourceTable + "#transform=externaldata" + "')";
            location = 3400;
            extLocation = extLocation + "\n" + "FORMAT 'TEXT' (delimiter '|' null 'null' escape '\\\\')";
            */
            // Modified : Execute nzsql to get data from Netezza
            String extLocation =    "EXECUTE E'/usr/local/nz/bin/nzsql -h " + sourceServer + " -port " + sourcePort + " -d " + sourceDatabase + " -u " + sourceUser
                    + " -W " + sourcePass + " -r -t -F \\'|\\' -A -c \"" + nzSQL  + sourceSchema + "." + sourceTable + "\" ' ON MASTER ";
            extLocation = extLocation + "\n" + "FORMAT 'CSV' (delimiter '|' quote '~' null 'null' escape E'\\\\');";
            location = 3400;

            ////////////////////////////////////////////
            //Add createSQL with Java Command to exec.
            ////////////////////////////////////////////
            location = 3500;
            createSQL = createSQL + extLocation;

            ////////////////////////////////////////////
            //Create new external web table
            ////////////////////////////////////////////
            location = 4000;
            if (debug)
                Logger.printMsg("Creating External Table: " + createSQL);

            stmt.executeUpdate(createSQL);

        }
        catch (SQLException ex)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + ex.getMessage() + ")");
        }
        catch (Exception e)
        {
            throw new SQLException("(" + myclass + ":" + method + ":" + location + ":" + e.getMessage() + ")");
        }
    }

}

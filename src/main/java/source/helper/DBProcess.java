package source.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
//import java.sql.Timestamp;

public class DBProcess {
//    public static String DB_URL = "jdbc:oracle:thin:@192.168.10.221:1521:DITECH";
    public static String DB_URL = "jdbc:oracle:thin:@localhost:1521:DATAIP";
    public static String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";

    public static Connection getConnection(String user, String pass) {
		Connection conn = null;
        try {
            if(conn == null || conn.isClosed()) {
                Class.forName(DB_DRIVER);
                conn = DriverManager.getConnection(DB_URL, user, pass);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
    
    public static void closeConnection(Connection connection, Statement statement, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (statement != null) {
                statement.close();
                statement = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = getConnection("vascms", "vascms");
            String sql = "select ISDN from log_request where rownum <= 10";
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String isdn = rs.getString("ISDN");
                System.out.println(isdn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection(conn, ps, rs);
        }
    }
}

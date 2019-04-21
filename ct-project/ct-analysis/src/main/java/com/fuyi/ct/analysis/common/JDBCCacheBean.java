package com.fuyi.ct.analysis.common;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 单例JDBCConnection
 */
public class JDBCCacheBean {
    private static Connection conn = null;
    private JDBCCacheBean() {}
    public static Connection getInstance() {
        try {
            if (conn != null) {
                return conn;
            }

            conn = JDBCUtil.getConnection();
//            if (conn == null || conn.isClosed() || conn.isValid(3)) {
//                conn = JDBCUtil.getConnection();
//            }
        } catch (SQLException e){
            e.printStackTrace();
        }
        return conn;
    }

}

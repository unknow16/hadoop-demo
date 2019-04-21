package com.fuyi.ct.analysis.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class JDBCUtil {

    public static void main(String[] args) throws Exception {
        Connection connection = getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("insert into tb_contacts( telephone, name) values (?, ?)");
        preparedStatement.setString(1, "123456");
        preparedStatement.setString(2, "hahah");
        preparedStatement.executeUpdate();
        System.out.println(connection);
    }

    private static final Logger logger = LoggerFactory.getLogger(JDBCUtil.class);
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://192.168.11.190:3306/db_telecom?useUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "123456";

    /**
     * 获取Mysql数据库的连接
     * @return
     */
    public static Connection getConnection() throws SQLException {
        try {
            Class<?> forName = Class.forName(MYSQL_DRIVER_CLASS);
            System.out.println("DriverClassName == " + forName.getName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
    }

    /**
     * 关闭数据库连接释放资源
     * @param connection
     * @param statement
     * @param resultSet
     */
    public static void close(Connection connection, Statement statement, ResultSet resultSet){
        if(resultSet != null) try {
            resultSet.close();
        } catch (SQLException e) {
        }

        if(statement != null) try {
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if(connection != null) try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

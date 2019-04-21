package com.fuyi.ct.analysis.format;

import com.fuyi.ct.analysis.common.Constants;
import com.fuyi.ct.analysis.common.JDBCUtil;
import com.fuyi.ct.analysis.converter.impl.DimensionConverter;
import com.fuyi.ct.analysis.kv.base.BaseDimension;
import com.fuyi.ct.analysis.kv.base.BaseValue;
import com.fuyi.ct.analysis.kv.impl.ComDimension;
import com.fuyi.ct.analysis.kv.impl.CountDurationValue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLOutputFormat extends OutputFormat<BaseDimension, BaseValue> {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://192.168.11.190:3306/db_telecom?useUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "123456";

    @Override
    public RecordWriter<BaseDimension, BaseValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //创建jdbc连接
        Connection conn = null;
        try {
            try {
                Class<?> forName = Class.forName(MYSQL_DRIVER_CLASS);
                System.out.println("DriverClassName == " + forName.getName());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);

            //关闭自动提交，以便于批量提交
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return new MysqlRecordWriter(conn);
    }

    /**
     * 校验输出
     * @param jobContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        String name = taskAttemptContext.getConfiguration().get(FileOutputFormat.OUTDIR);
        Path output = name == null ? null : new Path(name);
        return new FileOutputCommitter(output, taskAttemptContext);
    }

    static class MysqlRecordWriter extends RecordWriter<BaseDimension, BaseValue> {
        private Connection conn = null;
        private DimensionConverter dimensionConverter = null;
        private PreparedStatement preparedStatement = null;
        private int batchNumber = 0;
        int count = 0;

        public MysqlRecordWriter(Connection conn) {
            this.conn = conn;
            this.batchNumber = Constants.JDBC_DEFAULT_BATCH_NUMBER;
            this.dimensionConverter = new DimensionConverter();
        }

        @Override
        public void write(BaseDimension key, BaseValue value) throws IOException, InterruptedException {
            try {
                // 统计当前PreparedStatement对象待提交的数据量
                String sql = "INSERT INTO `tb_call`(`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`) VALUES(?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `id_date_contact` = ? ;";
                if (preparedStatement == null) {
                    preparedStatement = conn.prepareStatement(sql);
                }
                // 本次sql
                int i = 0;
                ComDimension comDimension = (ComDimension) key;
                CountDurationValue countDurationValue = (CountDurationValue) value;

                int id_date_dimension = dimensionConverter.getDimensionId(comDimension.getDateDimension());
                int id_contact = dimensionConverter.getDimensionId(comDimension.getContactDimension());
                int call_sum = countDurationValue.getCallSum();
                int call_duration_sum = countDurationValue.getCallDurationSum();

                String id_date_contact = id_date_dimension + "_" + id_contact;

                preparedStatement.setString(++i, id_date_contact);
                preparedStatement.setInt(++i, id_date_dimension);
                preparedStatement.setInt(++i, id_contact);
                preparedStatement.setInt(++i, call_sum);
                preparedStatement.setInt(++i, call_duration_sum);

                preparedStatement.setString(++i, id_date_contact);
                preparedStatement.addBatch();
                //当前缓存了多少个sql语句等待批量执行，计数器
                count++;

                // 批量提交
                if (count >= this.batchNumber) {
                    preparedStatement.executeBatch(); // 批量提交
                    conn.commit(); // 连接提交
                    count = 0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                preparedStatement.executeBatch();
                this.conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                JDBCUtil.close(conn, preparedStatement, null);
            }
        }
    }
}

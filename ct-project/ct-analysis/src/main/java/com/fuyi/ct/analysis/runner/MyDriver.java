package com.fuyi.ct.analysis.runner;

import com.fuyi.ct.analysis.format.MySQLOutputFormat;
import com.fuyi.ct.analysis.kv.impl.ComDimension;
import com.fuyi.ct.analysis.kv.impl.CountDurationValue;
import com.fuyi.ct.analysis.mapper.CountDurationMapper;
import com.fuyi.ct.analysis.reducer.CountDurationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class MyDriver {
    public static void main(String[] args) throws Exception {

        // 1. 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2. 设置jar加载路径
        job.setJarByClass(MyDriver.class);

        // 3. 设置map和reduce类
        // 为job设置Mapper
        setHBaseInputConfig(job);

        // 为job设置Reducer
        job.setReducerClass(CountDurationReducer.class);
        job.setOutputKeyClass(ComDimension.class);
        job.setOutputValueClass(CountDurationValue.class);

        // 为job设置OutputFormat
        job.setOutputFormatClass(MySQLOutputFormat.class);
        job.addFileToClassPath(new Path("/mysql-connector-java-5.1.27.jar"));

        // 4. 设置map输出
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置最终输出kv类型
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);

        // 6. 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, args[0]);
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }

    private static void setHBaseInputConfig(Job job) {
        Configuration conf = job.getConfiguration();
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(conf);
            //如果表不存在则直接返回，抛个异常也挺好
            if (!admin.tableExists("ns_telecom:calllog"))
                throw new RuntimeException("Unable to find the specified table.");

            Scan scan = new Scan();
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("ns_telecom:calllog"));
            TableMapReduceUtil.initTableMapperJob("ns_telecom:calllog", scan, CountDurationMapper.class, ComDimension.class, Text.class, job, true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(admin != null) try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

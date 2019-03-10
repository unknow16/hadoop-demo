package com.fuyi.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 四个范性分别是：
 * 1. 文件中每一行的偏移量key
 * 2. 每行文本
 * 3. 输出key类型
 * 4. 输出value类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 获取一行数据
        String line = value.toString();
        // 2. 切分
        String[] words = line.split(" ");

        // 3. 输出
        for (String word : words) {
            outKey.set(word);
            context.write(outKey, outValue);
        }
    }
}

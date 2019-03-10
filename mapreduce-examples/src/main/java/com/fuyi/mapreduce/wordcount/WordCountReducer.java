package com.fuyi.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 四个范性分别是：
 * 1. mapper输入key类型
 * 2. mapper输入value类型
 * 3. reducer输出key类型
 * 4. reducer输入value类型
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int sum = 0;
    private IntWritable count = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // 累加
        for(IntWritable val : values) {
            sum += val.get();
        }

        // 输出
        count.set(sum);
        context.write(key, count);
    }
}

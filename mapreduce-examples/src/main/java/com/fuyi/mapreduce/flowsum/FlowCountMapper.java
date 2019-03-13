package com.fuyi.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text keyPhone = new Text();
    private FlowBean valueBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] split = line.split("\t");

        keyPhone.set(split[1]);

        valueBean.setUpFlow(Long.parseLong(split[split.length - 3]));
        valueBean.setDownFlow(Long.parseLong(split[split.length - 2]));

        context.write(keyPhone, valueBean);
    }
}

package com.fuyi.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long upSum = 0;
        long downSum = 0;

        for (FlowBean flowBean : values) {
            upSum += flowBean.getUpFlow();
            downSum += flowBean.getDownFlow();
        }

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upSum);
        flowBean.setDownFlow(downSum);
        context.write(key, flowBean);
    }
}

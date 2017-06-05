package com.sohu.rdc.infcdn.offline.mr.computeOfDBBusiness_line;

/**
 * Created by zengxiaosen on 2017/5/25.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DBRecordOfBusiness_line_Mapper extends Mapper<LongWritable, DBRecordOfBusiness_line, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    public void map(LongWritable key, DBRecordOfBusiness_line values, Context context) throws IOException, InterruptedException {
        outKey.set(values.getDomain_name());
        outValue.set(values.getDomain_code()+"|"+values.getBusiness_code());
        context.write(outKey, outValue);
    }
}

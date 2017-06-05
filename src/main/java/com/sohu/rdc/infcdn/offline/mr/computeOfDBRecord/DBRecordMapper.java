package com.sohu.rdc.infcdn.offline.mr.computeOfDBRecord;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zengxiaosen on 2017/5/25.
 */
public class DBRecordMapper extends Mapper<LongWritable, DBRecordOfIp, Text, Text>{
    private Text outKey = new Text();
    private Text outValue = new Text();
    public void map(LongWritable key, DBRecordOfIp values, Context context) throws IOException, InterruptedException {
        outKey.set(values.getRegion());
        outValue.set(values.getMinIp()+"|"+values.getMaxIp());
        context.write(outKey, outValue);
    }
}


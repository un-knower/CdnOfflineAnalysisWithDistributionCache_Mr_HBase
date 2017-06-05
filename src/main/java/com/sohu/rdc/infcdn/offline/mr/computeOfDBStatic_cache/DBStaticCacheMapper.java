package com.sohu.rdc.infcdn.offline.mr.computeOfDBStatic_cache;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zengxiaosen on 2017/5/25.
 */
public class DBStaticCacheMapper extends Mapper<LongWritable, DBStaticCache, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    public void map(LongWritable key, DBStaticCache values, Context context) throws IOException, InterruptedException {
        outKey.set(values.getIp());
        outValue.set("1");
        context.write(outKey, outValue);
    }
}

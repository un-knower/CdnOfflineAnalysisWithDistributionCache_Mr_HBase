package com.sohu.rdc.infcdn.offline.mr.computeOfDBBusiness_line;

/**
 * Created by zengxiaosen on 2017/5/25.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DBRecordOfBusiness_line_Reducer extends Reducer<Text, Text, Text, Text>{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Iterator<Text> itr = values.iterator(); itr.hasNext();){
            context.write(key, itr.next());
        }
    }
}

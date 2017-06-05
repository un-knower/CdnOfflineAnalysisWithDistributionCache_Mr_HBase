package com.sohu.rdc.infcdn.offline.mr.comOfDestIp;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by yunhui li on 2017/5/19.
 *
 * 输入:
 * key:
 * ts_5m|00000|00
 * value:
 * requestNum|responseTimeStr|bodySizeStt|XX2_Result|XX3_Result|XX4_Result|XX5_Result
 *
 * 输出:
 * key:
 * ts_5m|00000|00
 * value:
 * requestNum|responseTimeStr|bodySizeStt|XX2_Result|XX3_Result|XX4_Result|XX5_Result
 *
 */
public class DestIpReducer extends Reducer<Text, Text, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(DestIpReducer.class);

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        long requestNumResult = 0;
        double responseTimeResult = 0;
        long bodySizeResult = 0;
        long XX2_Result = 0;
        long XX3_Result = 0;
        long XX4_Result = 0;
        long XX5_Result = 0;

        for (Text value : values) {
            String[] fields = StringUtils.split(value.toString(), "|");
            long requestNum = Long.valueOf(fields[0]);
            double responseTime = Double.valueOf(fields[1]);
            long bodySize = Long.valueOf(fields[2]);
            long XX2 = Long.valueOf(fields[3]);
            long XX3 = Long.valueOf(fields[4]);
            long XX4 = Long.valueOf(fields[5]);
            long XX5 = Long.valueOf(fields[6]);

            requestNumResult += requestNum;
            responseTimeResult += responseTime;
            bodySizeResult += bodySize;
            XX2_Result += XX2;
            XX3_Result += XX3;
            XX4_Result += XX4;
            XX5_Result += XX5;
        }

        context.write(key, new Text(requestNumResult +
            "|" + responseTimeResult + "|" + bodySizeResult + "|" + XX2_Result + "|" + XX3_Result
            + "|" + XX4_Result + "|" + XX5_Result));
    }
}

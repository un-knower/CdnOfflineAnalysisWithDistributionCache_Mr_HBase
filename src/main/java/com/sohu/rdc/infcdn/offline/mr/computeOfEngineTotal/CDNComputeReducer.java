package com.sohu.rdc.infcdn.offline.mr.computeOfEngineTotal;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by yunhui li on 2017/5/19.
 */
public class CDNComputeReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(CDNComputeReducer.class);

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        /*
        String.valueOf(dt_5m.getMillis()) + OUTVALUE_SEPA + machine + OUTVALUE_SEPA
                + requestNum + OUTVALUE_SEPA
                + responseTimeStr + OUTVALUE_SEPA + srcIPStr + OUTVALUE_SEPA + statusCode + OUTVALUE_SEPA
                + bodySizeStr + OUTVALUE_SEPA + method + OUTVALUE_SEPA + url + OUTVALUE_SEPA +
                dstIPStr + OUTVALUE_SEPA + contentType + referUrl + OUTVALUE_SEPA + browser +
                OUTVALUE_SEPA + StateOfIp + OUTVALUE_SEPA + stateCode + OUTVALUE_SEPA + engine +
                OUTVALUE_SEPA + engineRoom + OUTVALUE_SEPA + domain + OUTVALUE_SEPA + domainCode +
                OUTVALUE_SEPA + business + OUTVALUE_SEPA + logType + OUTVALUE_SEPA + serverRoom +
                OUTVALUE_SEPA + XX2_Result + OUTVALUE_SEPA + XX3_Result + OUTVALUE_SEPA
                + XX4_Result + OUTVALUE_SEPA + XX5_Result
         */

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

        /*
        outKey.set(String.valueOf(dt_5m.getMillis()) + OUTKEY_SEPA +
        domainCode + OUTKEY_SEPA + stateCode);

         */
        /*
        outKey.set(String.valueOf(dt_5m.getMillis()) + OUTKEY_SEPA + "0" + OUTKEY_SEPA + "2" +
                OUTKEY_SEPA + "0000" + OUTKEY_SEPA + engineRoom);
         */
        /*
        val rowkey = DateUtils.genDayTS(ts).toString + engineRoom + engine + logType
        ts->writeFlag->logType->engine->engineRoom
        val row = (flow, bandwidth, latency, request, XX2, XX3, XX4, XX5)
         */


        context.write(key, new Text(  requestNumResult +
                "|" + responseTimeResult + "|" + bodySizeResult + "|" + XX2_Result + "|" + XX3_Result
                + "|" + XX4_Result + "|" + XX5_Result));
    }
}

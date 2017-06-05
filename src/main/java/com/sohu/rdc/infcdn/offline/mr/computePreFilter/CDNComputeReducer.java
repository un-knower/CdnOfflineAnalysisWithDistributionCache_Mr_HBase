package com.sohu.rdc.infcdn.offline.mr.computePreFilter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by yunhui li on 2017/5/19.
 */
public class CDNComputeReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(CDNComputeReducer.class);

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        /**
         * MAP端：
         outKey.set(String.valueOf(dt_1m.getMillis()) + OUTKEY_SEPA + String.valueOf(dt_5m.getMillis()) + OUTVALUE_SEPA + machine + OUTVALUE_SEPA
         + requestNum + OUTVALUE_SEPA
         + responseTimeStr + OUTVALUE_SEPA + srcIPStr + OUTVALUE_SEPA + statusCode + OUTVALUE_SEPA
         + bodySizeStr + OUTVALUE_SEPA + method + OUTVALUE_SEPA + url + OUTVALUE_SEPA +
         dstIPStr + OUTVALUE_SEPA + contentType + referUrl + OUTVALUE_SEPA + browser +
         OUTVALUE_SEPA + StateOfIp + OUTVALUE_SEPA + stateCode + OUTVALUE_SEPA + engine +
         OUTVALUE_SEPA + engineRoom + OUTVALUE_SEPA + domain + OUTVALUE_SEPA + domainCode +
         OUTVALUE_SEPA + business + OUTVALUE_SEPA + logType + OUTVALUE_SEPA + serverRoom +
         OUTVALUE_SEPA + XX2_Result + OUTVALUE_SEPA + XX3_Result + OUTVALUE_SEPA
         + XX4_Result + OUTVALUE_SEPA + XX5_Result);
         outValue.set("1");
         */

        for(Iterator<Text> itr = values.iterator(); itr.hasNext();){
            context.write(key, itr.next());
        }
    }
}

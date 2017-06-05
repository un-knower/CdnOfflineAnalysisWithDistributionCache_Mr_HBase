package com.sohu.rdc.infcdn.offline.mr.resultOfEngineRoom;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;

import java.io.IOException;

/**
 * Created by yunhui li on 2017/5/22.
 *
 * 输入:
 * key:
 * ts_5m|00000|00
 * value:
 * requestNum|responseTimeStr|bodySizeStt|XX2_Result|XX3_Result|XX4_Result|XX5_Result
 *
 * 输出：
 * key:
 * ts_1d|0000|00
 * value:
 * ts_5m|requestNum|responseTimeStr|bodySizeStt|XX2_Result|XX3_Result|XX4_Result|XX5_Result
 *
 */
public class CombineKeyMapper extends Mapper<Object, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    private static final String SEPA = "\t";
    private static final String KEY_SEPA = "|";
    private static final String VALUE_SEPA = "|";

    @Override
    public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
        String keyString = StringUtils.substringBefore(value.toString(), SEPA);
        String valueString = StringUtils.substringAfter(value.toString(), SEPA);

        String[] fields = StringUtils.split(keyString, KEY_SEPA);
        /*
        ts + "|" + businessCode + "|" + stateCode + "|" + domainCode
        String ts = key.toString();
        String businessCode = "000";
        String stateCode = "00";
        String domainCode = "00000";
         */

        /*
        outKey.set(String.valueOf(dt_5m.getMillis()) + OUTKEY_SEPA + "0" + OUTKEY_SEPA + "2" +
                OUTKEY_SEPA + "0000" + OUTKEY_SEPA + engineRoom);
         */
        /*
        val rowkey = DateUtils.genDayTS(ts).toString + engineRoom + engine + logType
        ts->writeFlag->logType->engine
        val row = (flow, bandwidth, latency, request, XX2, XX3, XX4, XX5)
         */
        String tsStr = fields[0];
        String engineRoom = fields[4];
        String engine = fields[3];
        String logType = fields[2];




        long tsInMs = Long.valueOf(tsStr);
        DateTime dt = new DateTime(tsInMs);

        DateTime dt_1d = dt.dayOfMonth().roundFloorCopy();

        outKey.set(String.valueOf(dt_1d.getMillis()) + KEY_SEPA + engineRoom + KEY_SEPA + engine + KEY_SEPA + logType);
        outValue.set(tsStr + VALUE_SEPA + valueString);

        context.write(outKey, outValue);
    }

}

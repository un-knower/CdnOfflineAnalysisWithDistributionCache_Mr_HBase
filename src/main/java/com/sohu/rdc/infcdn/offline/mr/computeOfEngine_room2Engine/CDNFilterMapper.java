package com.sohu.rdc.infcdn.offline.mr.computeOfEngine_room2Engine;

import com.sohu.rdc.infcdn.offline.mr.entity.IPRegion;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yunhui li on 2017/5/18.
 */
public class CDNFilterMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(CDNFilterMapper.class);

    private static final String SEPA = " ";
    private static final String OUTKEY_SEPA = "|";
    private static final String OUTVALUE_SEPA = "|";

    private Text outKey = new Text();
    private Text outValue = new Text();

    private int errorCauseMissField;
    private int errorCauseNotNum;
    private int errorCauseStatusCode;
    private int errorCauseURL;
    private int errorCauseMethod;
    private int totalNum;
    private ArrayList<IPRegion> ipList;
    private Map<String, ArrayList<String>> businessmap;
    private HashMap<String, Integer> staticMap;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);


    }


    //  2:20s
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {

        /**
         * outKey.set(String.valueOf(dt_1m.getMillis()) + OUTKEY_SEPA + String.valueOf(dt_5m.getMillis()) +
         * OUTVALUE_SEPA + machine + OUTVALUE_SEPA + requestNum + OUTVALUE_SEPA
         + responseTimeStr + OUTVALUE_SEPA + srcIPStr + OUTVALUE_SEPA + statusCode + OUTVALUE_SEPA
         + bodySizeStr + OUTVALUE_SEPA + method + OUTVALUE_SEPA + url + OUTVALUE_SEPA +
         dstIPStr + OUTVALUE_SEPA + stateCode + OUTVALUE_SEPA + engine +
         OUTVALUE_SEPA + engineRoom + OUTVALUE_SEPA + domainCode +
         OUTVALUE_SEPA + business + OUTVALUE_SEPA + logType + OUTVALUE_SEPA + serverRoom +
         OUTVALUE_SEPA + XX2_Result + OUTVALUE_SEPA + XX3_Result + OUTVALUE_SEPA
         + XX4_Result + OUTVALUE_SEPA + XX5_Result);
         outValue.set("1");
         */
        String valueOfdata = value.toString().trim();
        String keyString = StringUtils.split(valueOfdata, "\t")[0];
        String[] keyTuple = StringUtils.split(keyString, "|");
        if(keyTuple.length != 22){
            LOG.info("ExceptionLog: log length is not 22: " + valueOfdata.toString());
            return;
        }
        //StringTokenizer st = new StringTokenizer(keyString, "|");
        for(int i=0; i< 20; i++){
            System.out.println("aaaa" + valueOfdata.toString());
            LOG.trace("aaaa" + valueOfdata.toString());
        }
//        String dt_1m = st.nextToken();
//        String dt_5m = st.nextToken();
//        String machine = st.nextToken();
//        String requestNum = st.nextToken();
//        String responseTimeStr = st.nextToken();
//        String srcIPStr = st.nextToken();
//        String statusCode = st.nextToken();
//        String bodySizeStr = st.nextToken();
//        String method = st.nextToken();
//        String url = st.nextToken();
//        String dstIPStr = st.nextToken();
//        String stateCode = st.nextToken();
//        String engine = st.nextToken();
//        String engineRoom = st.nextToken();
//        String domainCode = st.nextToken();
//        String business = st.nextToken();
//        String logType = st.nextToken();
//        String serverRoom = st.nextToken();
//        String XX2_Result = st.nextToken();
//        String XX3_Result = st.nextToken();
//        String XX4_Result = st.nextToken();
//        String XX5_Result = st.nextToken();

        String dt_1m = keyTuple[0];
        String dt_5m = keyTuple[1];
        String machine = keyTuple[2];
        String requestNum = keyTuple[3];
        String responseTimeStr = keyTuple[4];
        String srcIPStr = keyTuple[5];
        String statusCode = keyTuple[6];
        String bodySizeStr = keyTuple[7];
        String method = keyTuple[8];
        String url = keyTuple[9];
        String dstIPStr = keyTuple[10];
        String stateCode = keyTuple[11];
        String engine = keyTuple[12];
        String engineRoom = keyTuple[13];
        String domainCode = keyTuple[14];
        String business = keyTuple[15];
        String logType = keyTuple[16];
        String serverRoom = keyTuple[17];
        String XX2_Result = keyTuple[18];
        String XX3_Result = keyTuple[19];
        String XX4_Result = keyTuple[20];
        String XX5_Result = keyTuple[21];

        String nginxLog = value.toString();
        /*
        val rowkey = DateUtils.genDayTS(ts).toString + engineRoom + engine + logType
        ts->writeFlag->logType->engine->engineRoom
        val row = (flow, bandwidth, latency, request, XX2, XX3, XX4, XX5)
         */

        outKey.set(String.valueOf("000000" + OUTKEY_SEPA + "0" + OUTKEY_SEPA + "2" +
                OUTKEY_SEPA + engine + OUTKEY_SEPA + engineRoom));


        outValue.set(requestNum + OUTVALUE_SEPA
                + responseTimeStr + OUTVALUE_SEPA + bodySizeStr
                + OUTVALUE_SEPA + XX2_Result + OUTVALUE_SEPA + XX3_Result + OUTVALUE_SEPA
                + XX4_Result + OUTVALUE_SEPA + XX5_Result + OUTVALUE_SEPA );

        context.write(outKey, outValue);
    }




//    private static String generateRowKey(double tsInSecond) {
//        int tsKey = genDayTS((long) tsInSecond);
//        String originRowKey = tsKey + "00000" + "00";
//        MessageDigest md5 = null;
//        try {
//            md5 = MessageDigest.getInstance("MD5");
//        } catch (NoSuchAlgorithmException e) {
//            LOG.error("error: ", e);
//        }
//        md5.update(originRowKey.getBytes());
//        String newRowKey = new BigInteger(1, md5.digest()).toString(16);
//        return newRowKey;
//    }
}
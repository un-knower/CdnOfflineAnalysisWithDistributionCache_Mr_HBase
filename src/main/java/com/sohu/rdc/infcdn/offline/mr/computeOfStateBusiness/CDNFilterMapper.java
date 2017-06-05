package com.sohu.rdc.infcdn.offline.mr.computeOfStateBusiness;

import com.sohu.rdc.infcdn.offline.mr.entity.IPRegion;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yunhui li on 2017/5/18.
 */
public class CDNFilterMapper extends Mapper<Object, Text, Text, Text> {

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
    public void map(Object key, Text value, Context context) throws IOException,
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
            return;
        }
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
        outKey.set(dt_5m + OUTKEY_SEPA + business + OUTKEY_SEPA + "00" + OUTKEY_SEPA + "00000");
//        outValue.set(String.valueOf(dt_5m.getMillis()) + OUTVALUE_SEPA + machine + OUTVALUE_SEPA
//                + requestNum + OUTVALUE_SEPA
//                + responseTimeStr + OUTVALUE_SEPA + srcIPStr + OUTVALUE_SEPA + statusCode + OUTVALUE_SEPA
//                + bodySizeStr + OUTVALUE_SEPA + method + OUTVALUE_SEPA + url + OUTVALUE_SEPA +
//                dstIPStr + OUTVALUE_SEPA + contentType + referUrl + OUTVALUE_SEPA + browser +
//                OUTVALUE_SEPA + StateOfIp + OUTVALUE_SEPA + stateCode + OUTVALUE_SEPA + engine +
//                OUTVALUE_SEPA + engineRoom + OUTVALUE_SEPA + domain + OUTVALUE_SEPA + domainCode +
//                OUTVALUE_SEPA + business + OUTVALUE_SEPA + logType + OUTVALUE_SEPA + serverRoom +
//                OUTVALUE_SEPA + XX2_Result + OUTVALUE_SEPA + XX3_Result + OUTVALUE_SEPA
//                + XX4_Result + OUTVALUE_SEPA + XX5_Result);

//        System.out.println(String.valueOf(dt_5m.getMillis()) + OUTVALUE_SEPA + machine + OUTVALUE_SEPA
//                + requestNum + OUTVALUE_SEPA
//                + responseTimeStr + OUTVALUE_SEPA + srcIPStr + OUTVALUE_SEPA + statusCode + OUTVALUE_SEPA
//                + bodySizeStr + OUTVALUE_SEPA + method + OUTVALUE_SEPA + url + OUTVALUE_SEPA +
//                dstIPStr + OUTVALUE_SEPA + contentType + referUrl + OUTVALUE_SEPA + browser +
//                OUTVALUE_SEPA + StateOfIp + OUTVALUE_SEPA + stateCode + OUTVALUE_SEPA + engine +
//                OUTVALUE_SEPA + engineRoom + OUTVALUE_SEPA + domain + OUTVALUE_SEPA + domainCode +
//                OUTVALUE_SEPA + business + OUTVALUE_SEPA + logType + OUTVALUE_SEPA + serverRoom +
//                OUTVALUE_SEPA + XX2_Result + OUTVALUE_SEPA + XX3_Result + OUTVALUE_SEPA
//                + XX4_Result + OUTVALUE_SEPA + XX5_Result);


        outValue.set(requestNum + OUTVALUE_SEPA
                + responseTimeStr + OUTVALUE_SEPA + bodySizeStr
                + OUTVALUE_SEPA + XX2_Result + OUTVALUE_SEPA + XX3_Result + OUTVALUE_SEPA
                + XX4_Result + OUTVALUE_SEPA + XX5_Result + OUTVALUE_SEPA );

        context.write(outKey, outValue);
    }



    // 返回ms单位的整天值
    private static long genTsMy(double tsInSecond) {
        // 5min
        int inteval_5m = 5 * 60;
        long roundTs = (long) ((Math.floor(tsInSecond / inteval_5m)) * inteval_5m * 1000);
        // 规整成5分钟
        DateTime dt_5m = new DateTime(roundTs);
        // 规整成1天
        DateTime dt_1d = dt_5m.dayOfMonth().roundFloorCopy();
        return dt_1d.getMillis();
    }


    private static int genDayTS(long ts) {
        int offset = 8 * 60 * 60;
        return (int) ((ts + offset) / (60 * 60 * 24));
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
package com.sohu.rdc.infcdn.offline.mr.comOfEngine;

import com.sohu.rdc.infcdn.offline.mr.entity.IPRegion;
import org.apache.commons.lang.StringUtils;
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
 * 输入日志格式:
 * 04:30:26 local_nginx@gz-rm_102_21 1494880226.155 0.000 61.145.71.170 -/200 2428 GET
 * https://css.tv.itc.cn/wemedia/global/images/cpr02.png - DIRECT/10.20.102.122:80(0.000)
 * image/png "http://my.tv.sohu.com/pl/9087704/88930018.shtml"
 * "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)
 * Chrome/47.0.2526.108 Safari/537.36 2345Explorer/7.2.0.13379"
 *
 * 输出:
 * key:
 * ts_5m|00000|00
 * value:
 * requestNum|responseTimeStr|bodySizeStt|XX2_Result|XX3_Result|XX4_Result|XX5_Result
 *
 */
public class EngineMapper extends Mapper<Object, Text, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(EngineMapper.class);

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
    public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
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

        outKey.set(dt_1m+OUTKEY_SEPA+engineRoom+OUTKEY_SEPA+"00000");
        outValue.set(requestNum + OUTVALUE_SEPA
            + responseTimeStr + OUTVALUE_SEPA + bodySizeStr
            + OUTVALUE_SEPA + XX2_Result + OUTVALUE_SEPA + XX3_Result + OUTVALUE_SEPA
            + XX4_Result + OUTVALUE_SEPA + XX5_Result);

        context.write(outKey, outValue);
    }


}
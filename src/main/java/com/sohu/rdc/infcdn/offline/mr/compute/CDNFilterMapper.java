package com.sohu.rdc.infcdn.offline.mr.compute;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    //  2:20s
    @Override
    public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {

        String nginxLog = value.toString();
        totalNum++;

        String regex = "(.*\\d+:\\d+:\\d+)? ?(\\S+[@| ]\\S+) (\\d+.?\\d+) (\\d+.?\\d+)" +
            " (\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) (-/\\d+) (\\d+) (\\w{3,6})" +
            " (\\w{3,5}://\\S+) - (\\S+) (\\S+[;|; ]?[^\"]*?[;|; ]?[^\"]*?)" +
            " ([\"]\\S*[\"])( [\"].*[\"])?";

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(nginxLog);

        String timeStr = "";
        String machine = "";
        String tsStr = "";
        String responseTimeStr = "";
        String srcIPStr = "";
        String statusStr = "";
        String bodySizeStr = "";
        String method = "";
        String url = "";
        String dstIPStr = "";
        String contentType = "";
        String referUrl = "";
        String browser = "";

        // 字段个数不匹配
        if (!matcher.find()) {
            errorCauseMissField++;
            LOG.info("ExceptionLog errorCauseMissField: " + value.toString());
            return;
        }
        timeStr = matcher.group(1);
        machine = matcher.group(2);
        tsStr = matcher.group(3);
        responseTimeStr = matcher.group(4);
        srcIPStr = matcher.group(5);
        statusStr = matcher.group(6);
        bodySizeStr = matcher.group(7);
        method = matcher.group(8);
        url = matcher.group(9);
        dstIPStr = matcher.group(10);
        contentType = matcher.group(11);
        referUrl = matcher.group(12);
        browser = matcher.group(13);

        double tsInSecond = 0;
        long bodySize = 0;
        double responseTime = 0;
        try {
            tsInSecond = Double.valueOf(tsStr);
            bodySize = Long.valueOf(bodySizeStr);
            responseTime = Double.valueOf(responseTimeStr);
        } catch (Exception e) {
            // ts，bodySize，responseTime字段不是数值类型，认为是非法
            LOG.info("ExceptionLog errorCauseNotNum: " + value.toString());
            errorCauseNotNum++;
            return;
        }

        String statusCode = StringUtils.removeStart(statusStr, "-/");

        // 状态码不是 2XX,3XX,4XX,5XX 是非法的日志
        if (!StringUtils.startsWith(statusCode, "2") && !StringUtils.startsWith(statusCode, "3")
            && !StringUtils.startsWith(statusCode, "4")
            && !StringUtils.startsWith(statusCode, "5")) {
            LOG.info("ExceptionLog errorCauseStatusCode: " + value.toString());
            errorCauseStatusCode++;
            return;
        }

        // 必须是http或https
        if (!StringUtils.contains(url, "http://") && !StringUtils.contains(url, "https://")) {
            LOG.info("ExceptionLog errorCauseURL: " + value.toString());
            errorCauseURL++;
            return;
        }
        // 只处理GET和POST,HEAD,DELETE, OPTIONS
        if (!StringUtils.contains(method, "GET") && !StringUtils.contains(method, "POST")
            && !StringUtils.contains(method, "HEAD") && !StringUtils.contains(method, "DELETE")
            && !StringUtils.contains(method, "OPTIONS")) {
            LOG.info("ExceptionLog errorCauseMethod: " + value.toString());
            errorCauseMethod++;
            return;
        }


        int inteval_5m = 5 * 60;
        long roundTs_5m = (long) ((Math.floor(tsInSecond / inteval_5m)) * inteval_5m * 1000);
        DateTime dt_5m = new DateTime(roundTs_5m);
        DateTime dt_1d = dt_5m.dayOfMonth().roundFloorCopy();

        long requestNum = 1L;
        long XX2_Result = 0L;
        long XX3_Result = 0L;
        long XX4_Result = 0L;
        long XX5_Result = 0L;
        if (StringUtils.startsWith(statusCode, "2")) {
            XX2_Result = 1;
        } else if (StringUtils.startsWith(statusCode, "3")) {
            XX3_Result = 1;
        }
        if (StringUtils.startsWith(statusCode, "4")) {
            XX4_Result = 1;
        }
        if (StringUtils.startsWith(statusCode, "5")) {
            XX5_Result = 1;
        }
        //domainStr + KEY_SEPA + stateStr
        outKey.set(String.valueOf(dt_5m.getMillis())+OUTKEY_SEPA+"00000"+OUTKEY_SEPA+"00");
        outValue.set(requestNum + OUTVALUE_SEPA
            + responseTimeStr + OUTVALUE_SEPA + bodySizeStr
            + OUTVALUE_SEPA + XX2_Result + OUTVALUE_SEPA + XX3_Result + OUTVALUE_SEPA
            + XX4_Result + OUTVALUE_SEPA + XX5_Result + OUTVALUE_SEPA );

        context.write(outKey, outValue);
    }

    @Override
    protected void cleanup(Mapper<Object, Text, Text, Text>.Context context) throws
        IOException, InterruptedException {
        LOG.info("total=" + totalNum);
        LOG.info("errorCauseMissField=" + errorCauseMissField);
        LOG.info("errorCauseNotNum=" + errorCauseNotNum);
        LOG.info("errorCauseStatusCode=" + errorCauseStatusCode);
        LOG.info("errorCauseMethod=" + errorCauseMethod);
        LOG.info("errorCauseURL=" + errorCauseURL);
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

    private static String generateRowKey(double tsInSecond) {
        int tsKey = genDayTS((long) tsInSecond);
        String originRowKey = tsKey + "00000" + "00";
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            LOG.error("error: ", e);
        }
        md5.update(originRowKey.getBytes());
        String newRowKey = new BigInteger(1, md5.digest()).toString(16);
        return newRowKey;
    }
}
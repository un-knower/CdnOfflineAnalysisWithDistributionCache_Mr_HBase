package com.sohu.rdc.infcdn.offline.mr.result;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * Created by yunhui li on 2017/5/22.
 * 输入：
 * key:
 * ts_1d|0000|00
 * value:
 * ts_5m|requestNum|responseTimeStr|bodySizeStt|XX2_Result|XX3_Result|XX4_Result|XX5_Result
 *
 * 输出：hbase
 *
 */
public class HBaseTableReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTableReducer.class);

    private static final String SEPA = "\t";
    private static final String KEY_SEPA = "|";
    private static final String VALUE_SEPA = "|";

    private static Map<Integer, Long> requestNumResult = Maps.newHashMap();
    private static Map<Integer, Double> responseTimeResult = Maps.newHashMap();
    private static Map<Integer, Long> bodySizeResult = Maps.newHashMap();
    private static Map<Integer, Long> XX2Result = Maps.newHashMap();
    private static Map<Integer, Long> XX3Result = Maps.newHashMap();
    private static Map<Integer, Long> XX4Result = Maps.newHashMap();
    private static Map<Integer, Long> XX5Result = Maps.newHashMap();

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        for (Text value : values) {
            String[] valueFields = StringUtils.split(value.toString(), VALUE_SEPA);

            long ts = Long.valueOf(valueFields[0]);
            long requestNum = Long.valueOf(valueFields[1]);
            double responseTime = Double.valueOf(valueFields[2]);
            long bodySize = Long.valueOf(valueFields[3]);
            long XX2_num = Long.valueOf(valueFields[4]);
            long XX3_num = Long.valueOf(valueFields[5]);
            long XX4_num = Long.valueOf(valueFields[6]);
            long XX5_num = Long.valueOf(valueFields[7]);

            DateTime dt = new DateTime(ts);

            // 每5分钟1个点，这里计算是当天的第几个点
            int pointOfDay = dt.getHourOfDay() * 12 + dt.getMinuteOfHour() / 5 + 1;

            saveToMap(requestNumResult, pointOfDay, requestNum);
            saveToMap(responseTimeResult, pointOfDay, responseTime);
            saveToMap(bodySizeResult, pointOfDay, bodySize);
            saveToMap(XX2Result, pointOfDay, XX2_num);
            saveToMap(XX3Result, pointOfDay, XX3_num);
            saveToMap(XX4Result, pointOfDay, XX4_num);
            saveToMap(XX5Result, pointOfDay, XX5_num);
        }

        String tableName = context.getConfiguration().get("tableName");

        HTable table = new HTable(context.getConfiguration(), tableName);

        String[] keyFields = StringUtils.split(key.toString(), KEY_SEPA);

        /*
        outKey.set(String.valueOf(dt_1d.getMillis()) + KEY_SEPA + domainStr + KEY_SEPA + stateStr);
         */
        String tsStr = keyFields[0];
        String domainStr = keyFields[1];
        String stateStr = keyFields[2];

        long tsInSecond = Long.valueOf(tsStr) / 1000;

        int tsKey = genDayTS(tsInSecond);

        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String rawRowKey = tsKey + domainStr + stateStr;
        md5.update(rawRowKey.getBytes());
        String newRowKey = new BigInteger(1, md5.digest()).toString(16);


        Get get = new Get(Bytes.toBytes(newRowKey));

        Result result = table.get(get);
        if (!result.isEmpty()) {
            LOG.warn("key=" + newRowKey + " is not null, update history result");
            //
            for (int i = 1; i <= 288; i++) {
                updateToLongMap(requestNumResult, i, result, "request");
                updateToDoubleMap(responseTimeResult, i, result, "latency");
                updateToLongMap(bodySizeResult, i, result, "flow");
                updateToLongMap(XX2Result, i, result, "2XX");
                updateToLongMap(XX3Result, i, result, "3XX");
                updateToLongMap(XX4Result, i, result, "4XX");
                updateToLongMap(XX5Result, i, result, "5XX");
            }
        }

        Put put = new Put(Bytes.toBytes(newRowKey.toString()));

        for (int i = 1; i <= 288; i++) {
            saveToPut(requestNumResult, "request", i, put);
            saveToPut(responseTimeResult, "latency", i, put);
            saveToPut(bodySizeResult, "flow", i, put);
            saveToPut(XX2Result, "2XX", i, put);
            saveToPut(XX3Result, "3XX", i, put);
            saveToPut(XX4Result, "4XX", i, put);
            saveToPut(XX5Result, "5XX", i, put);
        }
        context.write(null, put);
    }

    private static int genDayTS(long ts) {
        int offset = 8 * 60 * 60;
        return (int) ((ts + offset) / (60 * 60 * 24));
    }

    private Map<Integer, Long> saveToMap(Map<Integer, Long> map, int key, long delta) {
        if (map.get(key) != null) {
            map.put(key, map.get(key) + delta);
        } else {
            map.put(key, delta);
        }
        return map;
    }

    private Map<Integer, Double> saveToMap(Map<Integer, Double> map, int key, double delta) {
        if (map.get(key) != null) {
            map.put(key, map.get(key) + delta);
        } else {
            map.put(key, delta);
        }
        return map;
    }

    private void saveToPut(Map map, String cf, int column, Put put) {
        if (map.get(column) != null) {
            put.add(Bytes.toBytes(cf), Bytes.toBytes(String.valueOf(column)), Bytes
                .toBytes(map.get(column).toString()));
        } else {
            put.add(Bytes.toBytes(cf), Bytes.toBytes(String.valueOf(column)), Bytes
                .toBytes("0"));
        }
    }

    private void updateToLongMap(Map<Integer, Long> map, int key, Result result, String cf) {
        if (map.get(key) != null) {
            map.put(key, map.get(key)
                + Long.valueOf(new String(result.getValue(cf.getBytes(), String.valueOf(key)
                .getBytes()))));
        } else {
            map.put(key, Long.valueOf(new String(result.getValue(cf.getBytes(), String.valueOf(key)
                .getBytes()))));
        }
    }

    private void updateToDoubleMap(Map<Integer, Double> map, int key, Result result, String cf) {
        if (map.get(key) != null) {
            map.put(key, map.get(key)
                + Double.valueOf(new String(result.getValue(cf.getBytes(), String.valueOf(key)
                .getBytes()))));
        } else {
            map.put(key, Double.valueOf(new String(result.getValue(cf.getBytes(), String.valueOf(key)
                .getBytes()))));
        }
    }
}
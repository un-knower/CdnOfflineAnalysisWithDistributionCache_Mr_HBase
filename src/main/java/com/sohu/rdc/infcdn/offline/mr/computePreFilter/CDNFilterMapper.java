package com.sohu.rdc.infcdn.offline.mr.computePreFilter;

import com.sohu.rdc.infcdn.offline.mr.Utils.IPService;
import com.sohu.rdc.infcdn.offline.mr.Utils.IPUtils;
import com.sohu.rdc.infcdn.offline.mr.Utils.RoomMapUtils;
import com.sohu.rdc.infcdn.offline.mr.Utils.UrlUtils;
import com.sohu.rdc.infcdn.offline.mr.entity.IPRegion;
import com.sohu.rdc.infcdn.offline.mr.entity.NginxEvent;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        /* compiled code */
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        if(context.getCacheFiles() != null && context.getCacheFiles().length > 0){
            URI[] cacheFile = context.getCacheFiles();
            Path dbrecordPath = new Path(cacheFile[0]);
            Path dbBusiness_linePath = new Path(cacheFile[1]);
            Path dbstaticCachePath = new Path(cacheFile[2]);

            ipList = GetIpList(dbrecordPath, context);
            businessmap = GetBusinessMap(dbBusiness_linePath, context);
            staticMap = GetStaticMap(dbstaticCachePath, context);
        }
    }

    private HashMap<String,Integer> GetStaticMap(Path dbrecordFile, Context context) throws IOException {
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        FileSystem fileSystem = dbrecordFile.getFileSystem(context.getConfiguration());
        FSDataInputStream fin = fileSystem.open(dbrecordFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
        String s = null;
        while((s = bufferedReader.readLine()) != null){
            String[] s_split = s.split("\t");
            System.out.println("ip: " + s_split[0]);
            System.out.println("value: " + s_split[1]);
            String ip = s_split[0];
            Integer value = Integer.valueOf(s_split[1]);
            result.put(ip, value);
        }
        bufferedReader.close();
        //fileReader.close();
        fin.close();
        return result;
    }

    private Map<String,ArrayList<String>> GetBusinessMap(Path dbrecordFile, Context context) throws IOException {
        FileSystem fileSystem = dbrecordFile.getFileSystem(context.getConfiguration());
        FSDataInputStream fin = fileSystem.open(dbrecordFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
        String s;
        Map<String, ArrayList<String>> businessMap = new HashMap<String, ArrayList<String>>();
        while((s = bufferedReader.readLine()) != null){
            String[] s_split = s.split("\t");
            System.out.println("domain_name: " + s_split[0]);
            System.out.println("domaincode_businesscode: " + s_split[1]);
            String domain_name = s_split[0];
            String domaincode_businesscode = s_split[1];
            String[] domaincode_businesscode_split = domaincode_businesscode.split("\\|");
            String domain_code = domaincode_businesscode_split[0];
            String business_code = domaincode_businesscode_split[1];
            ArrayList<String> temp = new ArrayList<String>();
            temp.add(domain_code);
            temp.add(business_code);
            businessMap.put(domain_name, temp);
        }

        bufferedReader.close();
        fin.close();
        return businessMap;
    }

    private ArrayList<IPRegion> GetIpList(Path dbrecordFile, Context context) throws IOException {
        FileSystem fileSystem = dbrecordFile.getFileSystem(context.getConfiguration());
        FSDataInputStream fin = fileSystem.open(dbrecordFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
        String s;
        ArrayList<IPRegion> IpRegionList = new ArrayList<IPRegion>();
        while((s=bufferedReader.readLine()) != null){
            String[] s_split = s.split("\t");
            LOG.info("region: " + s_split[0]);
            LOG.info("minIp_maxIp: " + s_split[1]);
            String region = s_split[0];
            String minIp_maxIp = s_split[1];
            String[] minIp_maxIp_split = minIp_maxIp.split("\\|");
            String minIp = minIp_maxIp_split[0];
            String maxIp = minIp_maxIp_split[1];
            IPRegion ipRegion = new IPRegion();
            Long minIpLong = ipToLong(minIp);
            Long maxIpLong = ipToLong(maxIp);
            ipRegion.setMinIP(minIpLong);
            ipRegion.setMaxIP(maxIpLong);
            ipRegion.setRegion(region);
            IpRegionList.add(ipRegion);

        }
        bufferedReader.close();
        fin.close();
        //fileReader.close();
        return IpRegionList;
    }
    private static Long ipToLong(String ipAddress) {
        String[] ipAddressTuple = ipAddress.split("[.]");
        int j=0;
        long sum = 0;
        for(int i=ipAddressTuple.length-1; i>0; i--){
            Long reuslt = Long.valueOf((long) (Integer.valueOf(ipAddressTuple[i]) * Math.pow(Double.valueOf(256), Double.valueOf(j))));
            sum += reuslt;
            j++;
        }
        return  sum;
    }
    //  2:20s
    @Override
    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {

        String nginxLog = value.toString();
        totalNum++;

        System.out.println(ipList);
        System.out.println(businessmap);
        System.out.println(staticMap);
        IPService ipService = new IPService();
        ipService.setIpList(ipList);

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
        String StateOfIp = "";
        String stateCode = "";
        String engine = "";
        String engineRoom = "";
        String domain = "";
        String domainCode = "";
        String business = "";
        String logType = "";
        String serverRoom = "";
        // 字段个数不匹配
        if (!matcher.find()) {
            errorCauseMissField++;
            //输出到hdfs的Data_exception.log
            //ExceptionOutputToHdfsLog(exceptionLogFile);
            LOG.info("ExceptionLog errorCauseMissField: " + value.toString());
            return;
        }
        NginxEvent event = new NginxEvent();

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

        if(ipService.getLocationWithAddress(srcIPStr) != null){
            StateOfIp = ipService.getLocationWithAddress(srcIPStr);
        }else{
            StateOfIp = "other";
        }
        if(machine.contains("local_nginx@") && machine.contains("_")){
            String str1 = StringUtils.substring(machine, 12);
            String str2 = StringUtils.split(str1, "_")[0];
            engine = str1;
            //engineRoom = roomMap.get
            HashMap<String, String> RoomMap = RoomMapUtils.getRoomMap();
            //engineRoom = RoomMap.get(str2);
            if(RoomMap.get(str2) != null){
                engineRoom = RoomMap.get(str2);
            }else{
                engineRoom = "99";
            }
        }

        double tsInSecond = 0;
        long bodySize = 0;
        double responseTime = 0;
        try {
            tsInSecond = Double.valueOf(tsStr);
            bodySize = Long.valueOf(bodySizeStr);
            responseTime = Double.valueOf(responseTimeStr);
        } catch (Exception e) {
            // ts，bodySize，responseTime字段不是数值类型，认为是非法
            errorCauseNotNum++;
            LOG.info("ExceptionLog errorCauseNotNum: " + value.toString());
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
            errorCauseURL++;
            LOG.info("ExceptionLog errorCauseURL: " + value.toString());
            return;
        }
        // 只处理GET和POST,HEAD,DELETE, OPTIONS
        if (!StringUtils.contains(method, "GET") && !StringUtils.contains(method, "POST")
                && !StringUtils.contains(method, "HEAD") && !StringUtils.contains(method, "DELETE")
                && !StringUtils.contains(method, "OPTIONS")) {
            errorCauseMethod++;
            LOG.info("ExceptionLog errorCauseMethod: " + value.toString());
            return;
        }
        String host = UrlUtils.getHost(url);
        int length = StringUtils.split(host, '.').length;
        for(int i=0; i< length; i++){
            if(businessmap.get(host)==null){
                domainCode = "99999";
                domain = "other";
                business = "999";
            }else{
                domainCode = businessmap.get(host).get(0);
                domain = host;
                business = businessmap.get(host).get(1);
            }
        }
        int inteval_1m_0 = 1 * 60;
        long roundTs_1m_0 = (long) ((Math.floor(tsInSecond / inteval_1m_0)) * inteval_1m_0 * 1000);
        DateTime dt_1m = new DateTime(roundTs_1m_0);


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
        int tempResultOfdstIPStr = 0;

        if(dstIPStr != null && !dstIPStr.isEmpty()){
            if(staticMap.get(dstIPStr) != null){
                tempResultOfdstIPStr = 1;
            }
        }else{
            System.out.println("dstIpStr为空!!!!!!!!!!!!!!!!!!!!!!!!");
        }

        if(tempResultOfdstIPStr == 1){
            logType = "0";
            serverRoom = engineRoom;
        }else{
            logType = "1";
            serverRoom = "01";
        }
        HashMap<String, String> ipstateMap = IPUtils.GetIpMap();
        stateCode = ipstateMap.get(StateOfIp);
        if(stateCode == null){
            stateCode = "other";
        }
        outKey.set(String.valueOf(dt_1m.getMillis()) + OUTKEY_SEPA + String.valueOf(dt_5m.getMillis()) + OUTVALUE_SEPA + machine + OUTVALUE_SEPA
                + requestNum + OUTVALUE_SEPA
                + responseTimeStr + OUTVALUE_SEPA + srcIPStr + OUTVALUE_SEPA + statusCode + OUTVALUE_SEPA
                + bodySizeStr + OUTVALUE_SEPA + method + OUTVALUE_SEPA + url + OUTVALUE_SEPA +
                dstIPStr + OUTVALUE_SEPA + stateCode + OUTVALUE_SEPA + engine +
                OUTVALUE_SEPA + engineRoom + OUTVALUE_SEPA + domainCode +
                OUTVALUE_SEPA + business + OUTVALUE_SEPA + logType + OUTVALUE_SEPA + serverRoom +
                OUTVALUE_SEPA + XX2_Result + OUTVALUE_SEPA + XX3_Result + OUTVALUE_SEPA
                + XX4_Result + OUTVALUE_SEPA + XX5_Result);
        outValue.set("1");
        context.write(outKey, outValue);
    }
    @Override
    protected void cleanup(Context context) throws
            IOException, InterruptedException {
        LOG.info("total=" + totalNum);
        LOG.info("errorCauseMissField=" + errorCauseMissField);
        LOG.info("errorCauseNotNum=" + errorCauseNotNum);
        LOG.info("errorCauseStatusCode=" + errorCauseStatusCode);
        LOG.info("errorCauseMethod=" + errorCauseMethod);
        LOG.info("errorCauseURL=" + errorCauseURL);
    }
}
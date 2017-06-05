package com.sohu.rdc.infcdn.offline.mr.Utils;

import com.sohu.rdc.infcdn.offline.mr.entity.IPRegion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zengxiaosen on 2017/5/24.
 */
public class MysqlUtils {

    public static Connection getConn() throws SQLException {
        //连接类，得到和目标数据库连接的对象
        Connection con=null;
        //加载驱动
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //加载驱动类
            con = DriverManager.getConnection("jdbc:mysql://10.31.73.48:3306/hue",
                    "hue_user", "hue_test");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return con;
    }

    public static HashMap<String, Integer> getStaticCacheMap(){
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        Connection con = null;
        try{
            con = getConn();
            ResultSet set = null;
            PreparedStatement prepare = con.prepareStatement("SELECT * FROM cdn_static_cache");
            //把sql语句发送到数据库，得到预编译类的对象，这句话是选择该student表里的所有数据
            set=prepare.executeQuery();
            //将得到的数据库响应的查询结果存放在ResultSet对象中
            while(set.next()){
                String cache = set.getString("ip");
                result.put(cache, 1);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public static HashMap<String, Integer> getStaticCacheMap1(Mapper.Context context) throws IOException {
        HashMap<String, Integer> result = new HashMap<String, Integer>();

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/xiaoliu/dbentity/dbstaticCache/part-r-00000");
        if(fs.exists(path)){
            FSDataInputStream is = fs.open(path);
            ////使用BufferedReader 进行readLine
            BufferedReader d = new BufferedReader(new InputStreamReader(is));
            String s = d.readLine();
            while(s!= null){

                String[] s_split = s.split("\t");
                System.out.println("ip: " + s_split[0]);
                System.out.println("value: " + s_split[1]);
                String ip = s_split[0];
                Integer value = Integer.valueOf(s_split[1]);
                result.put(ip, value);
                s = d.readLine();
            }

        }
        return result;
    }



//    public static void main(String[] args){
//        HashMap<String, Integer> test = MysqlUtils.getStaticCacheMap();
//        for(Map.Entry<String, Integer> entry : test.entrySet()){
//            System.out.println(entry.getKey() + " : " + entry.getValue());
//        }}


    public static ArrayList<IPRegion> getIpArray() {
        ArrayList<IPRegion> ipRegionsList = new ArrayList<IPRegion>();
        Connection con = null;

        try{
            con = getConn();
            ResultSet set = null;
            PreparedStatement prepare = con.prepareStatement("SELECT * FROM ipseg_new");
            //把sql语句发送到数据库，得到预编译类的对象，这句话是选择该student表里的所有数据
            set=prepare.executeQuery();
            //将得到的数据库响应的查询结果存放在ResultSet对象中
            while(set.next()){
                String minIP = set.getString("minIP");
                String maxIP = set.getString("maxIP");
                String region = set.getString("region");
                IPRegion ipRegion = new IPRegion();

                ipRegion.setMinIP(ipToLong(minIP));
                ipRegion.setMaxIP(ipToLong(maxIP));
                ipRegion.setRegion(region);
                ipRegionsList.add(ipRegion);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return ipRegionsList;
    }

    public static Map<String, ArrayList<String>> getBusinessMap(){
        Map<String, ArrayList<String>> businessMap = new HashMap<String, ArrayList<String>>();
        Connection con = null;
        try{
            con = getConn();
            ResultSet set = null;
            PreparedStatement prepare = con.prepareStatement("SELECT * FROM business_line");
            //把sql语句发送到数据库，得到预编译类的对象，这句话是选择该student表里的所有数据
            set=prepare.executeQuery();
            //将得到的数据库响应的查询结果存放在ResultSet对象中
            while(set.next()){
                String domain_name = set.getString("domain_name");
                String domain_code = set.getString("domain_code");
                String business_code = set.getString("business_code");
                ArrayList<String> temp = new ArrayList<String>();
                temp.add(domain_code);
                temp.add(business_code);
                businessMap.put(domain_name, temp);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return businessMap;

    }

    public static Map<String, ArrayList<String>> getBusinessMap1(Mapper.Context context) throws IOException {
        Map<String, ArrayList<String>> businessMap = new HashMap<String, ArrayList<String>>();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/xiaoliu/dbentity/dbBusiness_line/part-r-00000");
        if(fs.exists(path)){
            FSDataInputStream is = fs.open(path);
            ////使用BufferedReader 进行readLine
            BufferedReader d = new BufferedReader(new InputStreamReader(is));
            String s = d.readLine();
            while(s!=null){

                String[] s_split = s.split("\t");
                System.out.println("domain_name: " + s_split[0]);
                System.out.println("domaincode_businesscode: " + s_split[1]);
                String domain_name = s_split[0];
                String domaincode_businesscode = s_split[1];
                String[] domaincode_businesscode_split = domaincode_businesscode.split("|");
                String domain_code = domaincode_businesscode_split[0];
                String business_code = domaincode_businesscode_split[1];
                ArrayList<String> temp = new ArrayList<String>();
                temp.add(domain_code);
                temp.add(business_code);
                businessMap.put(domain_name, temp);
                s = d.readLine();
            }

        }
        return businessMap;

    }

    public static ArrayList<IPRegion> getIpArray1(Mapper.Context context) throws IOException {
        //Configuration conf = new Configuration();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/xiaoliu/dbentity/dbrecord/part-r-00000");
        ArrayList<IPRegion> IpRegionList = new ArrayList<IPRegion>();
        if(fs.exists(path)){
                FSDataInputStream is = fs.open(path);
            ////使用BufferedReader 进行readLine
            BufferedReader d = new BufferedReader(new InputStreamReader(is));
            String s = d.readLine();
            while(s!=null){
                String[] s_split = s.split("\t");
                System.out.println("region: " + s_split[0]);
                System.out.println("minIp_maxIp: " + s_split[1]);
                String region = s_split[0];
                String minIp_maxIp = s_split[1];
                String[] minIp_maxIp_split = minIp_maxIp.split("|");
                String minIp = minIp_maxIp_split[0];
                String maxIp = minIp_maxIp_split[1];
                IPRegion ipRegion = new IPRegion();
                Long minIpLong = ipToLong(minIp);
                Long maxIpLong = ipToLong(maxIp);
                ipRegion.setMinIP(minIpLong);
                ipRegion.setMaxIP(maxIpLong);
                ipRegion.setRegion(region);
                IpRegionList.add(ipRegion);
                s = d.readLine();
            }


        }

        return IpRegionList;

    }

    private static Long ipToLong(String ipAddress) {
        String[] ipAddressTuple = ipAddress.split("\\.");
        int j=0;
        long sum = 0;
        for(int i=ipAddressTuple.length-1; i>0; i--){
            Long reuslt = Long.valueOf((long) (Integer.valueOf(ipAddressTuple[i]) * Math.pow(Double.valueOf(256), Double.valueOf(j))));
            sum += reuslt;
            j++;
        }

        return  sum;
    }
}

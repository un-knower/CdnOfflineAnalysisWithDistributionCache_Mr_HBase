package com.sohu.rdc.infcdn.offline.test;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by zhengyangliu on 2017/6/1.
 */
public class GenerateRowKey {
    public static void main(String[] args){
        long  ts = 0L;
        String rowKeyStr ="fdsfdf";
        int offSetTs = genDayTS(ts);
        System.out.println( generateRowKey(String.valueOf(offSetTs),rowKeyStr));
    }
   public static String  generateRowKey(String rowKeyTs, String rowKeyStr){
        String newRowKey = "";
       MessageDigest md5 = null;
       try {
           md5 = MessageDigest.getInstance("MD5");
       } catch (NoSuchAlgorithmException e) {
           e.printStackTrace();
       }
       md5.update((rowKeyTs+rowKeyStr).getBytes());
       newRowKey = new BigInteger(1, md5.digest()).toString(16);
        return newRowKey;
    }
    private static int genDayTS(long ts) {
        int offset = 8 * 60 * 60;
        return (int) ((ts + offset) / (60 * 60 * 24));
    }
}

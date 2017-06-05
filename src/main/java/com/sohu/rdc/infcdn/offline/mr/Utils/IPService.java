package com.sohu.rdc.infcdn.offline.mr.Utils;

import com.sohu.rdc.infcdn.offline.mr.entity.IPRegion;

import java.util.ArrayList;

/**
 * Created by zengxiaosen on 2017/5/24.
 */
public class IPService {
    public ArrayList<IPRegion> getIpList() {
        return ipList;
    }

    public void setIpList(ArrayList<IPRegion> ipList) {
        this.ipList = ipList;
    }

    private ArrayList<IPRegion> ipList;

    public  String getLocationWithAddress(String address){
        Long ip = ipToLong(address);
        String state = binarySearchIp(ip);
        return state;
    }

    public  String binarySearchIp(Long ip) {
        int left = 0;
        int right = ipList.size();
        int mid = -1;
        while (left < right){
            mid = (left + right) / 2;
            if(ip > ipList.get(mid).getMaxIP()){
                left = mid + 1;
            }else if(ip < ipList.get(mid).getMinIP()){
                right = mid;
            }else{
                return ipList.get(mid).getRegion();
            }
        }
        return "other";
    }

    public  Long ipToLong(String ipAddress) {
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

    /*
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
     */

    public  String longToIp(Long count){
        ArrayList<String> as = new ArrayList<String>();
        for(int i=0; i<4; i++){
            double temp0 = Math.floor(Math.pow(Double.valueOf(256), i));
            int temp1 = (int) (temp0);
            int temp2 = temp1 % 256;
            int temp3 = (int)(count / temp2);
            as.add(String.valueOf(temp3));
        }
        String result = "";
        for(int i=as.size()-1; i>0; i--){
            result += as.get(i);
            if(i != 0){
                result += ".";
            }
        }

        return result;

    }

}

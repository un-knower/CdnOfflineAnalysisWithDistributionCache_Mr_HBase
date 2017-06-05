package com.sohu.rdc.infcdn.offline.mr.Utils;

/**
 * Created by zengxiaosen on 2017/5/24.
 */
public class UrlUtils {
    public static String getHost(String url) {
        char[] charArray = url.toCharArray();
        int count = 0;
        int left = 0;
        int right = 1;
        StringBuilder stringBuilder = new StringBuilder();
        for(int i=0; i< charArray.length-1; i++){
            if(charArray[i] == '/'){
                count +=1;
                if(count == 2){
                    left = i;
                }
                if(count == 3){
                    right = i;
                }
                if(count >= 4){
                    break;
                }
            }
        }
        for(int j=left+1; j< right; j++){
            stringBuilder.append(charArray[j]);
        }
        String result = stringBuilder.toString();
        return result;
    }
}

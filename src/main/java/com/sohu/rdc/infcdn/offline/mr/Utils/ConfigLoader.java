package com.sohu.rdc.infcdn.offline.mr.Utils;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zengxiaosen on 2017/5/24.
 */


public class ConfigLoader {

    public static void main(String[] args){
        Properties properties = new Properties();
        InputStream inputStream = ConfigLoader.class.getResourceAsStream("/application.properties");
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String url = properties.getProperty("test.db.url");
        String username = properties.getProperty("test.db.username");
        String password = properties.getProperty("test.db.password");

    }
}

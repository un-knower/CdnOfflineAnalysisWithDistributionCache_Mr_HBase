package com.sohu.rdc.infcdn.offline.mr.computeOfDBStatic_cache;

/**
 * Created by zengxiaosen on 2017/5/25.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class DBStaticCacheJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path output = new Path("/user/xiaoliu/dbentity/dbstaticCache");


        Configuration conf = new Configuration();
//        Configuration conf = HBaseConfiguration.create();
//        conf.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"));

        FileSystem fs = FileSystem.get(URI.create(output.toString()), conf);
        if (fs.exists(output)) {
            fs.delete(output);
        }
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://10.31.73.48:3306/hue", "hue_user", "hue_test");

        Job job = new Job(conf, "DBStaticCacheJob");
        job.setJarByClass(DBStaticCacheJob.class);
        job.setMapperClass(DBStaticCacheMapper.class);
        job.setReducerClass(DBStaticCacheReducer.class);
        //job.setCombinerClass(DBRecordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(DBInputFormat.class);
        FileOutputFormat.setOutputPath(job, output);
        //六个参数分别为：
        //1.Job;2.Class<? extends DBWritable>
        //3.表名;4.where条件
        //5.order by语句;6.列名
        String[] fields = {"ip"};
        DBInputFormat.setInput(job, DBStaticCache.class, "cdn_static_cache", "", "", fields);
        System.exit(job.waitForCompletion(true) ? 0 : 1);    }
}

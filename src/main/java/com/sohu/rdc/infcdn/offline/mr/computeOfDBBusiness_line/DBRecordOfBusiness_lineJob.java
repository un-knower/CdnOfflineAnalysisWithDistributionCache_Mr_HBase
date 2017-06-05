package com.sohu.rdc.infcdn.offline.mr.computeOfDBBusiness_line;

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

public class DBRecordOfBusiness_lineJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path output = new Path("/user/xiaoliu/dbentity/dbBusiness_line");


        Configuration conf = new Configuration();
//        Configuration conf = HBaseConfiguration.create();
//        conf.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"));

        FileSystem fs = FileSystem.get(URI.create(output.toString()), conf);
        if (fs.exists(output)) {
            fs.delete(output);
        }
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://10.31.73.48:3306/hue", "hue_user", "hue_test");

        Job job = new Job(conf, "DBRecordJob");
        job.setJarByClass(DBRecordOfBusiness_lineJob.class);
        job.setMapperClass(DBRecordOfBusiness_line_Mapper.class);
        job.setReducerClass(DBRecordOfBusiness_line_Reducer.class);
        //job.setCombinerClass(DBRecordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(DBInputFormat.class);
        FileOutputFormat.setOutputPath(job, output);
        //六个参数分别为：
        //1.Job;2.Class<? extends DBWritable>
        //3.表名;4.where条件
        //5.order by语句;6.列名
        String[] fields = {"id","domain_name", "business_line_name", "domain_code", "business_code"};
        DBInputFormat.setInput(job, DBRecordOfBusiness_line.class, "business_line", "", "", fields);
        System.exit(job.waitForCompletion(true) ? 0 : 1);    }
}

package com.sohu.rdc.infcdn.offline.mr.comOfDestIp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yunhui li on 2017/5/15.
 */
public class DestIpJob {

    private static final Logger LOG = LoggerFactory.getLogger(DestIpJob.class);

    public static void main(String[] args) throws Exception {

        LOG.debug("start");
        Configuration conf = new Configuration();
//        Configuration conf = HBaseConfiguration.create();
//        conf.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"));

        LOG.debug("got conf");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: CDNServer3Job <input> <output>");
            System.exit(2);
        }

        Job job = new Job(conf, "CDNServer3Job");
        job.setJarByClass(DestIpJob.class);
        job.setMapperClass(DestIpMapper.class);

        job.setReducerClass(DestIpReducer.class);
        job.setCombinerClass(DestIpReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        //将hdfs上的文件加入分布式缓存
//        job.addCacheFile(new URI("/user/xiaoliu/dbentity/dbrecord/part-r-00000"));
//        job.addCacheFile(new URI("/user/xiaoliu/dbentity/dbBusiness_line/part-r-00000"));
//        job.addCacheFile(new URI("/user/xiaoliu/dbentity/dbstaticCache/part-r-00000"));

        //分布式缓存要存储的文件路径
        String cachePath[] = {
                "/user/xiaoliu/dbentity/dbrecord/part-r-00000",
                "/user/xiaoliu/dbentity/dbBusiness_line/part-r-00000",
                "/user/xiaoliu/dbentity/dbstaticCache/part-r-00000"
        };

        //将hdfs上的文件加入分布式缓存
        job.addCacheFile(new Path(cachePath[0]).toUri());
        job.addCacheFile(new Path(cachePath[1]).toUri());
        job.addCacheFile(new Path(cachePath[2]).toUri());

//         mapreduce.job.inputformat.class=com.hadoop.mapreduce.LzoTextInputFormat
//        job.setInputFormatClass(LzoTextInputFormat.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,
            new Path(otherArgs[otherArgs.length - 1]));
//        FileInputFormat.addInputPath(job, new Path("D:\\source\\input\\inputlog.txt"));
//        FileOutputFormat.setOutputPath(job,
//            new Path("D:\\source\\output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

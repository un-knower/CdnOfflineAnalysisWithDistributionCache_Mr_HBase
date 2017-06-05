package com.sohu.rdc.infcdn.offline.mr.result;

import com.sohu.rdc.infcdn.offline.test.HBaseWriteJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by yunhui li on 2017/5/22.
 */
public class CDNResultToHBaseJob {
    private static final Logger LOG = LoggerFactory.getLogger(CDNResultToHBaseJob.class);

    public static void main(String[] args) throws Exception {

        LOG.debug("start");

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"));
        //conf.addResource(new Path("/user/xiaoliu/hbase-site.xml"));

        LOG.debug("got conf");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: CDNResultToHBaseJob <input> <table>");
            System.exit(2);
        }

        String tableName = otherArgs[1];
        conf.set("tableName", tableName);

        Job job = new Job(conf, "CDNResultToHBaseJob");

        job.setJarByClass(CDNResultToHBaseJob.class);

        LOG.debug("init credential");
        TableMapReduceUtil.initTableReducerJob(
            tableName,      // output table
            HBaseWriteJob.MyTableReducer.class,             // reducer class
            job);

        job.setMapperClass(CombineKeyMapper.class);
        job.setReducerClass(HBaseTableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

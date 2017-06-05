package tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by yunhui li on 2017/5/19.
 */
public class WriteResultToHBase {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: WriteResultToHBase <input> <table>");
            System.exit(2);
        }

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"));

        FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path(args[0]);

        InputStream inputStream = hdfs.open(path);

        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

        String tempString = null;

        HTable table = new HTable(conf, args[1]);

        int line = 1;
        // 一次读入一行，直到读入null为文件结束
        while ((tempString = br.readLine()) != null) {
            // 显示行号
            System.out.println("line " + line + ": " + tempString);
            line++;
            saveToHBase(tempString, table);
        }
        br.close();
        inputStream.close();
        hdfs.close();
    }

    private static void saveToHBase(String result, HTable table) {

//        Put put = new Put(Bytes.toBytes(key.toString()));
//        put.add(Bytes.toBytes(cf), Bytes.toBytes(String.valueOf(column)), Bytes
//            .toBytes(map.get(column).toString()));


    }
}

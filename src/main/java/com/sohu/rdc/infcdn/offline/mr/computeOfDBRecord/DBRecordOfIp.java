package com.sohu.rdc.infcdn.offline.mr.computeOfDBRecord;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by zengxiaosen on 2017/5/24.
 */
public class DBRecordOfIp implements Writable, DBWritable {
    /*
    String minIP = set.getString("minIP");
    String maxIP = set.getString("maxIP");
    String region = set.getString("region");
     */
    private String minIp;
    private String maxIp;
    private String region;

    public String getMinIp() {
        return minIp;
    }

    public void setMinIp(String minIp) {
        this.minIp = minIp;
    }

    public String getMaxIp() {
        return maxIp;
    }

    public void setMaxIp(String maxIp) {
        this.maxIp = maxIp;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.minIp = Text.readString(in);
        this.maxIp = Text.readString(in);
        this.region = Text.readString(in);
    }

    @Override
    public void write(PreparedStatement pst) throws SQLException {

    }

    @Override
    public void readFields(ResultSet set) throws SQLException {
        this.minIp = set.getString("minIP");
        this.maxIp = set.getString("maxIP");
        this.region = set.getString("region");
    }
}

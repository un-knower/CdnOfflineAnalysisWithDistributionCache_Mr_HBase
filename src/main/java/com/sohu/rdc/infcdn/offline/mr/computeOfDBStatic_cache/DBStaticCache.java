package com.sohu.rdc.infcdn.offline.mr.computeOfDBStatic_cache;

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
 * Created by zengxiaosen on 2017/5/25.
 */
public class DBStaticCache implements Writable, DBWritable {
    private String ip;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.ip = Text.readString(in);

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {

    }

    @Override
    public void readFields(ResultSet set) throws SQLException {
        this.ip = set.getString("ip");
    }
}

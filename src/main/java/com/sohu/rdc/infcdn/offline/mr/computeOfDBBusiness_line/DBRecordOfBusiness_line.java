package com.sohu.rdc.infcdn.offline.mr.computeOfDBBusiness_line;

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
public class DBRecordOfBusiness_line implements Writable, DBWritable {
    private int id;
    private String domain_name;
    private String business_line_name;
    private String domain_code;
    private String business_code;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDomain_name() {
        return domain_name;
    }

    public void setDomain_name(String domain_name) {
        this.domain_name = domain_name;
    }

    public String getBusiness_line_name() {
        return business_line_name;
    }

    public void setBusiness_line_name(String business_line_name) {
        this.business_line_name = business_line_name;
    }

    public String getDomain_code() {
        return domain_code;
    }

    public void setDomain_code(String domain_code) {
        this.domain_code = domain_code;
    }

    public String getBusiness_code() {
        return business_code;
    }

    public void setBusiness_code(String business_code) {
        this.business_code = business_code;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.domain_name = Text.readString(in);
        this.business_line_name = Text.readString(in);
        this.domain_code = Text.readString(in);
        this.business_code = Text.readString(in);
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {

    }

    @Override
    public void readFields(ResultSet set) throws SQLException {
        this.id = set.getInt("id");
        this.domain_name = set.getString("domain_name");
        this.business_line_name = set.getString("business_line_name");
        this.domain_code = set.getString("domain_code");
        this.business_code = set.getString("business_code");
    }
}

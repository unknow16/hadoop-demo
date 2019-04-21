package com.fuyi.ct.analysis.kv.impl;

import com.fuyi.ct.analysis.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 时间维度+联系人维度的组合维度
 */
public class ComDimension extends BaseDimension {
    //时间维度
    private DateDimension dateDimension = new DateDimension();
    //联系人维度
    private ContactDimension contactDimension = new ContactDimension();

    public ComDimension() {
        super();
    }

    public ComDimension(DateDimension dateDimension, ContactDimension contactDimension) {
        this.dateDimension = dateDimension;
        this.contactDimension = contactDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o) return 0;
        ComDimension comDimension = (ComDimension) o;

        int tmp = this.dateDimension.compareTo(comDimension.getDateDimension());
        if(tmp != 0) return tmp;

        tmp = this.contactDimension.compareTo(comDimension.getContactDimension());
        return tmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.dateDimension.write(dataOutput);
        this.contactDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.dateDimension.readFields(dataInput);
        this.contactDimension.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComDimension that = (ComDimension) o;

        if (dateDimension != null ? !dateDimension.equals(that.dateDimension) : that.dateDimension != null) return false;
        return contactDimension != null ? contactDimension.equals(that.contactDimension) : that.contactDimension == null;
    }

    @Override
    public int hashCode() {
        int result = dateDimension != null ? dateDimension.hashCode() : 0;
        result = 31 * result + (contactDimension != null ? contactDimension.hashCode() : 0);
        return result;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }
}

package com.fuyi.ct.analysis.converter;

import com.fuyi.ct.analysis.kv.base.BaseDimension;

import java.io.IOException;

/**
 * 转化接口，用于根据传入的维度对象，得到该维度对象对应的数据库主键id
 */
public interface IConverter {
    // 根据传入的dimension对象，获取数据库中对应该对象数据的id，如果不存在，则插入该数据再返回
    int getDimensionId(BaseDimension dimension) throws IOException;
}

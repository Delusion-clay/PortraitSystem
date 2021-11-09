package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.BrandEntity;
import com.it.userportrait.log.ScanOpertor;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

public class BrandMap implements MapFunction<String, BrandEntity> {


    @Override
    public BrandEntity  map(String s) throws Exception {
        ScanOpertor scanOpertor = JSONObject.parseObject(s, ScanOpertor.class);
        long userid = scanOpertor.getUserid();
        long productId = scanOpertor.getProductId();
        String tablename = "product";
        String rowkey = productId+"";
        String famliyname="info";
        String colum = "productbrand";
        String brand = HbaseUtils2.getdata(tablename,rowkey,famliyname,colum);
        BrandEntity brandEntity = new BrandEntity();
        brandEntity.setBrand(brand);
        long timeinfo = DateUtils.getCurrentHourStart(System.currentTimeMillis());
        String groupField = "brand=="+timeinfo+"=="+userid+"=="+brand;
        brandEntity.setGroupField(groupField);
        brandEntity.setNumbers(1l);
        brandEntity.setProductId(productId);
        brandEntity.setUserid(userid);
        brandEntity.setTimeinfo(timeinfo+"");
        String timeinfoString = DateUtils.getDateString(timeinfo,"yyyyMMddhh");
        brandEntity.setTimeinfoString(timeinfoString);

        return brandEntity;
    }
}

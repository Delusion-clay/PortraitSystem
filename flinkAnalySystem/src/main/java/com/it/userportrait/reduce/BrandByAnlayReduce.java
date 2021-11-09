package com.it.userportrait.reduce;

import com.it.userportrait.analy.BrandEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

public class BrandByAnlayReduce implements ReduceFunction<BrandEntity> {

    public BrandEntity reduce(BrandEntity brandEntity, BrandEntity t1) throws Exception {
        long numbers1 = brandEntity.getNumbers();
        long numbers2 = t1.getNumbers();

        String brand = brandEntity.getBrand();
        String timeinfo = brandEntity.getTimeinfo();
        String groupField = brandEntity.getGroupField();
        BrandEntity brandEntityfinal = new BrandEntity();
        brandEntityfinal.setBrand(brand);
        brandEntityfinal.setGroupField(groupField);
        brandEntityfinal.setNumbers(numbers1+numbers2);
        brandEntityfinal.setTimeinfo(timeinfo);
        return brandEntityfinal;
    }
}

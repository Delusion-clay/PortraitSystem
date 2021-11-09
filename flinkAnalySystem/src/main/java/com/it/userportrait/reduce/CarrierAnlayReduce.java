package com.it.userportrait.reduce;

import com.it.userportrait.analy.CarrierEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;

public class CarrierAnlayReduce implements ReduceFunction<CarrierEntity> {

    public CarrierEntity reduce(CarrierEntity carrierEntity, CarrierEntity t1) throws Exception {
        long numbers1 = 0l;
        String groupField = "";
        if(carrierEntity != null){
            numbers1 =carrierEntity.getNumbers();
            groupField = carrierEntity.getGroupField();
        }
        long numbers2 = 0l;
        if(t1 != null){
            numbers2 = t1.getNumbers();
            groupField = t1.getGroupField();
        }

        if(StringUtils.isNotBlank(groupField)){
            CarrierEntity carrierEntity1 = new CarrierEntity();
            carrierEntity1.setGroupField(groupField);
            carrierEntity1.setNumbers(numbers1+numbers2);
            return  carrierEntity1;
        }
        return null;
    }
}


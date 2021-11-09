package com.it.userportrait.map;

import com.it.userportrait.analy.BrushEntity;
import org.apache.flink.api.common.functions.MapFunction;

public class UserUseFisrtAddressMap implements MapFunction<BrushEntity, BrushEntity> {


    @Override
    public BrushEntity map(BrushEntity brushEntity) throws Exception {
        long userid = brushEntity.getUserid();
        long nubmers = brushEntity.getNumbers();
        String address = brushEntity.getAddress();
        String timeinfo = brushEntity.getTimeinfo();
        String groupField = "useruseFirstAddress=="+userid+"=="+timeinfo;
        BrushEntity brushEntity1 = new BrushEntity();
        brushEntity1.setAddress(address);
        brushEntity1.setTimeinfo(timeinfo);
        brushEntity1.setNumbers(nubmers);
        brushEntity1.setUserid(userid);
        brushEntity1.setGroupField(groupField);
        return brushEntity1;
    }
}

package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.BrushEntity;
import org.apache.flink.api.common.functions.MapFunction;

public class IllBrushMap implements MapFunction<BrushEntity, BrushEntity> {

    @Override
    public BrushEntity map(BrushEntity brushEntity) throws Exception {
        String address = brushEntity.getAddress();
        long userid = brushEntity.getUserid();
        String timeinfo = brushEntity.getTimeinfo();
        String timeinfoString = brushEntity.getTimeinfoString();
        BrushEntity brushEntity1 = new BrushEntity();
        brushEntity1.setAddress(address);
        brushEntity1.setUserid(userid);
        brushEntity1.setTimeinfo(timeinfo);
        brushEntity1.setTimeinfoString(timeinfoString);
        String groupField = "IllBrush=="+address+"=="+timeinfo;
        brushEntity1.setGroupField(groupField);
        return brushEntity1;
    }
}

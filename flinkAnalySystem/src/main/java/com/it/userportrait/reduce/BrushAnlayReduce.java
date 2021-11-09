package com.it.userportrait.reduce;

import com.it.userportrait.analy.BrushEntity;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class BrushAnlayReduce implements GroupReduceFunction<BrushEntity, BrushEntity> {

    @Override
    public void reduce(Iterable<BrushEntity> iterable, Collector<BrushEntity> collector) throws Exception {
        Iterator<BrushEntity> iterator =   iterable.iterator();
        long numbertotal = 0l;
        String address = "";
        long userid = -1l;
        String groupField = "";
        String timeinfo = "";
        while(iterator.hasNext()){
            BrushEntity brushEntity = iterator.next();
            address = brushEntity.getAddress();
            userid = brushEntity.getUserid();
            groupField = brushEntity.getGroupField();
            timeinfo = brushEntity.getTimeinfo();
            long numbers = brushEntity.getNumbers();
            numbertotal = numbertotal+numbers;
        }
        BrushEntity brushEntity1 = new BrushEntity();
        brushEntity1.setNumbers(numbertotal);
        brushEntity1.setAddress(address);
        brushEntity1.setTimeinfo(timeinfo);
        brushEntity1.setGroupField(groupField);
        brushEntity1.setUserid(userid);
        collector.collect(brushEntity1);
    }
}

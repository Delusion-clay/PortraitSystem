package com.it.userportrait.reduce;

import com.it.userportrait.analy.BrushEntity;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class IllBrushFenBuAnlayReduce implements GroupReduceFunction<BrushEntity, BrushEntity> {

    @Override
    public void reduce(Iterable<BrushEntity> iterable, Collector<BrushEntity> collector) throws Exception {
        Iterator<BrushEntity> iterator =   iterable.iterator();
        String illusernums = "";
        String timeinfo = "";
        long totalNums = 0l;
        Set<Long> useridSet = new HashSet<Long>();
        while(iterator.hasNext()){
            BrushEntity brushEntity = iterator.next();
            illusernums = brushEntity.getIllusernums();
            timeinfo = brushEntity.getTimeinfo();
            long numbetrs = brushEntity.getNumbers();
            totalNums += numbetrs;
        }
        BrushEntity brushEntity1 = new BrushEntity();
        brushEntity1.setTimeinfo(timeinfo);
        brushEntity1.setIllusernums(illusernums);
        brushEntity1.setNumbers(totalNums);
        collector.collect(brushEntity1);
    }
}

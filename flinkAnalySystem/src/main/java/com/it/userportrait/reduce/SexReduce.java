package com.it.userportrait.reduce;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-07-28 0:20
 */
import com.it.userportrait.analy.SexEntity;
import com.it.userportrait.util.CreateDataSet;
import com.it.userportrait.util.Logistic;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class SexReduce implements GroupReduceFunction<SexEntity, ArrayList<Double>> {
    @Override
    public void reduce(Iterable<SexEntity> iterable, Collector<ArrayList<Double>> collector) throws Exception {
        Iterator<SexEntity> data = iterable.iterator();
        CreateDataSet createDataSet = new CreateDataSet();
        while(data.hasNext()){
            SexEntity sexEntity = data.next();
            ArrayList<String> dats = new ArrayList<>();
            long userid = sexEntity.getUserid();
            long ordernums = sexEntity.getOrdernums();
            long orderintenums = sexEntity.getOrderintenums();
            long manClothes = sexEntity.getManClothes();
            long chidrenClothes = sexEntity.getChidrenClothes();
            long oldClothes = sexEntity.getOldClothes();
            long womenClothes = sexEntity.getWomenClothes();
            double ordermountavg = sexEntity.getOrdermountavg();
            long productscannums = sexEntity.getProductscannums();
            int label = sexEntity.getLabel();//0 女 1 男
            dats.add(userid+"");
            dats.add(ordernums+"");
            dats.add(orderintenums+"");
            dats.add(manClothes+"");
            dats.add(chidrenClothes+"");
            dats.add(oldClothes+"");
            dats.add(womenClothes+"");
            dats.add(ordermountavg+"");
            dats.add( productscannums+"");


            createDataSet.data.add(dats);
            createDataSet.labels.add(label+"");
        }
        ArrayList<Double> weights = new ArrayList<Double>();
        weights = Logistic.gradAscent1(createDataSet, createDataSet.labels, 500);
        collector.collect(weights);
    }
}


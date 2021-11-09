package com.it.userportrait.reduce;

import com.it.userportrait.analy.Point;
import com.it.userportrait.analy.UserGroupEntity;
import com.it.userportrait.util.Cluster;
import com.it.userportrait.util.KMeansRun;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-21 16:30
 */
public class UserFeatureGroupReduce implements GroupReduceFunction<UserGroupEntity, ArrayList<Point>> {
    @Override
    public void reduce(Iterable<UserGroupEntity> iterable, Collector< ArrayList<Point>> collector) throws Exception {
        Iterator<UserGroupEntity> iterator = iterable.iterator();
        List<UserGroupEntity> finalList = new ArrayList<UserGroupEntity>();
        ArrayList<float[]> dataSet = new ArrayList<float[]>();
        UserGroupEntity userGroupEntityFinal = null;
        while(iterator.hasNext()){
            UserGroupEntity userGroupEntity = iterator.next();
            double avgAmount = userGroupEntity.getAvgAmount();//
            double maxAmount = userGroupEntity.getMaxAmount();
            int avgdays = userGroupEntity.getAvgdays();//
            long dianZiNums = userGroupEntity.getDianZiNums();
            long shenghuoNums = userGroupEntity.getShenghuoNums();
            long huwaiNums = userGroupEntity.getHuwaiNums();
            long time1 = userGroupEntity.getTime1();//7-12 1
            long time2 = userGroupEntity.getTime2();//13-19 2
            long time3 = userGroupEntity.getTime3();//20-24 3
            long time4 = userGroupEntity.getTime4();//0-6 4
            float[] arraylFloat = new float[]{
                    Float.valueOf(avgAmount+""),Float.valueOf(maxAmount+""),Float.valueOf(avgdays+""),Float.valueOf(dianZiNums+"")
                    ,Float.valueOf(huwaiNums+""),Float.valueOf(shenghuoNums+""),Float.valueOf(time1+""),
                    Float.valueOf(time2+""),Float.valueOf(time3+""),Float.valueOf(time4+"")
            };

            dataSet.add(arraylFloat);
        }
        KMeansRun kRun =new KMeansRun(6, dataSet);

        Set<Cluster> clusterSet = kRun.run();
        ArrayList<Point> list = new ArrayList<Point>();
        for(Cluster cluster:clusterSet){
            list.add(cluster.getCenter());
        }
        collector.collect(list);
    }
}

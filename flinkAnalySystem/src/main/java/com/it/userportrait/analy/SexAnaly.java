package com.it.userportrait.analy;


import com.it.userportrait.map.SexMap;
import com.it.userportrait.map.SexSaveMap;
import com.it.userportrait.reduce.SexReduce;
import com.it.userportrait.reduce.SexSaveAnlayReduce;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.*;
/**
 * @Description //TODO 用户性别预测与周性别预测
 * @Date 0:33 2021/7/28
 * @Param
 * @return
 **/

public class SexAnaly {
    public static void main(String[] args) {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> fileSource =  env.readTextFile("hdfs://hadoop-101://train.file");

        DataSet<SexEntity> map  = fileSource.map(new SexMap());
        DataSet<ArrayList<Double>> reduce  = map.groupBy("groupField").reduceGroup(new SexReduce());
        ArrayList<Double> finalWeight = new ArrayList<Double>();
        try {
            Map<Integer,Double> weightmap = new TreeMap<Integer,Double>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });
            List<ArrayList<Double>> resultlist =  reduce.collect();
            int groupSize = resultlist.size();
            for(int i =0;i<resultlist.size();i++){
                ArrayList<Double> datainner = resultlist.get(i);
                Double predata0 = weightmap.get(0)==null?0.0d:weightmap.get(0);
                Double predata1 = weightmap.get(1)==null?0.0d:weightmap.get(1);
                weightmap.put(0,datainner.get(0)+predata0);
                weightmap.put(1,datainner.get(1)+predata1);
            }

            Set<Map.Entry<Integer,Double>> sets = weightmap.entrySet();
            for(Map.Entry<Integer,Double> set:sets){
                Integer index = set.getKey();
                Double weighttotal = set.getValue();
                Double weightfinal = weighttotal/groupSize;
                finalWeight.add(index,weightfinal);
            }
            DataSet<String> testfileSource =  env.readTextFile("hdfs://hadoop-101://test.file");
            DataSet<SexEntity> mapsave = testfileSource.map(new SexSaveMap(finalWeight));
            DataSet<SexEntity> reducesave = mapsave.groupBy("groupField").reduce(new SexSaveAnlayReduce());
            List<SexEntity> savelist = reducesave.collect();
            for(SexEntity youfanSexEntity:savelist){
                String groupyField = youfanSexEntity.getGroupField();
                String [] groupfileds = groupyField.split("==");
                String timeinfo = groupfileds[1];
                String sex = groupfileds[2];
                long numbers = youfanSexEntity.getNumbers();
                String tablename = "sexinfo";
                Map<String, String> dataMap = new HashMap<String, String>();
                dataMap.put("sex",sex);
                dataMap.put("timeinfo",timeinfo);
                dataMap.put("numbers",numbers+"");
                String timeinfoString = DateUtils.getDateString(Long.valueOf(timeinfo),"yyyyMMdd");
                dataMap.put("timeinfoString",timeinfoString+"");
                Set<String> intFieldSet = new HashSet<String>();
                intFieldSet.add("numbers");
                ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
            }
            env.execute("SexAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

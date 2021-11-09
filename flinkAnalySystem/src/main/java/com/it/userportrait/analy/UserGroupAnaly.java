package com.it.userportrait.analy;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.map.UserFeatureFinalGroupMap;
import com.it.userportrait.map.UserGroupFeatureMap;
import com.it.userportrait.map.UserGroupMap;
import com.it.userportrait.reduce.UserFeatureGroupReduce;
import com.it.userportrait.reduce.UserGroupFenBuReduce;
import com.it.userportrait.reduce.UserGroupReduce;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.Cluster;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.KMeansRun;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.*;

/**
 * @description: 用户分群
 * @author: Delusion
 * @date: 2021-08-21 16:21
 */
public class UserGroupAnaly {
    public static void main(String[] args) {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> fileSource =  env.readTextFile("hdfs://hadoop-101://train.file");

        DataSet<UserGroupEntity> map  = fileSource.map(new UserGroupMap());
        DataSet<UserGroupEntity> reduce  = map.groupBy("groupField").reduceGroup(new UserGroupReduce());
        DataSet<UserGroupEntity> featureMap = reduce.map(new UserGroupFeatureMap());
        DataSet<ArrayList<Point>> featureReduce = featureMap.reduceGroup(new UserFeatureGroupReduce());
        try {
            List<ArrayList<Point>> listset = featureReduce.collect();
            ArrayList<float[]> dataSet = new ArrayList<float[]>();

            for(ArrayList<Point> list :listset){
                for(Point point:list){
                    float[] local = point.getlocalArray();
                    dataSet.add(local);
                }
            }
            KMeansRun kRun =new KMeansRun(6, dataSet);
            Set<Cluster> clusterSetfinal = kRun.run();
            ArrayList<Point> pointArrayList = new ArrayList<Point>();
            int count=10000;
            for(Cluster cluster:clusterSetfinal){
                Point point = cluster.getCenter();
                point.setId(count++);
                pointArrayList.add(point);
            }
            DataSet<UserGroupEntity> finalFeatureMap = featureMap.map(new UserFeatureFinalGroupMap(pointArrayList));
            DataSet<UserGroupEntity> finalFeatureFenBuReduce = finalFeatureMap.groupBy("groupField").reduceGroup(new UserGroupFenBuReduce());
            List<UserGroupEntity> list = finalFeatureFenBuReduce.collect();
            for(UserGroupEntity userGroupEntity:list){
                Point point = userGroupEntity.getCenterPoint();
                long timeinfo = userGroupEntity.getTimeinfo();
                String timeinfoString = DateUtils.getDateString(timeinfo,"yyyyMM");
                long nubmers = userGroupEntity.getNumbers();
                long pointid = point.getId();
                String tablename = "groupFenbuinfo";
                Map<String, String> dataMap = new HashMap<String, String>();
                dataMap.put("point", JSONObject.toJSONString(point));
                dataMap.put("pointid",pointid+"");
                dataMap.put("timeinfo",timeinfo+"");
                dataMap.put("numbers",nubmers+"");
                dataMap.put("timeinfoString",timeinfoString+"");
                Set<String> intFieldSet = new HashSet<String>();
                intFieldSet.add("numbers");
                ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
            }
            env.execute("UserGroupAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

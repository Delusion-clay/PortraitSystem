package com.it.userportrait.analy;

import com.it.userportrait.map.BrushMap;
import com.it.userportrait.map.IllBrushMap;
import com.it.userportrait.map.UserUseFisrtAddressMap;
import com.it.userportrait.reduce.BrushAnlayReduce;
import com.it.userportrait.reduce.IllBrushAnlayReduce;
import com.it.userportrait.reduce.IllBrushFenBuAnlayReduce;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.*;

/**
 * @description: 恶意刷单，多地址
 * @author: Delusion
 * @date: 2021-08-10 10:10
 */
public class BrushAnaly {
    public static void main(String[] args) {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> fileSource =  env.readTextFile("hdfs://hadoop-101://orderinfo.file");
        DataSet<BrushEntity> map  = fileSource.map(new BrushMap());
        DataSet<BrushEntity> reduce  = map.groupBy("groupField").reduceGroup(new BrushAnlayReduce());
        DataSet<BrushEntity> usefirstaddMap = reduce.map(new UserUseFisrtAddressMap());
        DataSet<BrushEntity> usefirstaddreduce = usefirstaddMap.groupBy("groupField").reduceGroup(new BrushAnlayReduce());
        DataSet<BrushEntity> illBrushMap = reduce.map(new IllBrushMap());
        DataSet<BrushEntity> illBrushReduce = illBrushMap.groupBy("groupField").reduceGroup(new IllBrushAnlayReduce());
        DataSet<BrushEntity> illBrushFenBuReduce = illBrushReduce.groupBy("groupField").reduceGroup(new IllBrushFenBuAnlayReduce());
        try {
            List<BrushEntity> list = illBrushFenBuReduce.collect();
            for(BrushEntity brushEntity:list){
                String timeinfo = brushEntity.getTimeinfo();
                String illusernums = brushEntity.getIllusernums();
                long nubmers = brushEntity.getNumbers();
                String tablename = "illusernumsbyinfo";
                Map<String, String> dataMap = new HashMap<String, String>();
                dataMap.put("illusernums",illusernums);
                dataMap.put("timeinfo",timeinfo);
                dataMap.put("numbers",nubmers+"");
                String timeinfoString = DateUtils.getDateString(Long.valueOf(timeinfo),"yyyyMM");
                dataMap.put("timeinfoString",timeinfoString+"");
                Set<String> intFieldSet = new HashSet<String>();
                intFieldSet.add("numbers");
                ClickHouseUtils.insertData(tablename,dataMap,intFieldSet);
            }
            env.execute("BrushAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

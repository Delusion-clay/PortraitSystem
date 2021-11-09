package com.it.userportrait.reduce;

import com.it.userportrait.analy.BrushEntity;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class IllBrushAnlayReduce implements GroupReduceFunction<BrushEntity, BrushEntity> {

    @Override
    public void reduce(Iterable<BrushEntity> iterable, Collector<BrushEntity> collector) throws Exception {
        Iterator<BrushEntity> iterator =   iterable.iterator();
        String address = "";
        long userid = -1l;
        String groupField = "";
        String timeinfo = "";
        Set<Long> useridSet = new HashSet<Long>();
        while(iterator.hasNext()){
            BrushEntity brushEntity = iterator.next();
            address = brushEntity.getAddress();
            userid = brushEntity.getUserid();
            timeinfo = brushEntity.getTimeinfo();
            groupField = brushEntity.getGroupField();
            useridSet.add(userid);
        }

        String tablename = "userinfo";
        String famliyname="info";
        String colum = "illBrushUser";
        if(useridSet.size()>6){
            //恶意刷单用户
            for(long userId:useridSet){
                String rowkey = userId+"";
                HbaseUtils2.putdata(tablename,rowkey,famliyname,colum,"1");
            }
        }
        String illusernumsflag = "";
        int useridssize = useridSet.size();
        if(useridssize>6&&useridssize<=10){
            illusernumsflag = "10";
        }else if(useridssize>11&&useridssize<=15){
            illusernumsflag = "15";
        }else if(useridssize>15&&useridssize<=20){
            illusernumsflag = "20";
        }else if(useridssize>20){
            illusernumsflag = "20以上";
        }
        BrushEntity brushEntity1 = new BrushEntity();
        brushEntity1.setTimeinfo(timeinfo);
        String groupFieldnew = "illusernumsflag=="+illusernumsflag+"=="+timeinfo;
        brushEntity1.setGroupField(groupFieldnew);
        brushEntity1.setIllusernums(illusernumsflag);
        brushEntity1.setNumbers(1l);
        collector.collect(brushEntity1);
    }
}

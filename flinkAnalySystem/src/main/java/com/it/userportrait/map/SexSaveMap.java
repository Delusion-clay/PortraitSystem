package com.it.userportrait.map;

import com.it.userportrait.analy.SexEntity;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.HbaseUtils2;
import com.it.userportrait.util.Logistic;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-07-28 0:23
 */
public class SexSaveMap implements MapFunction<String, SexEntity> {
    private ArrayList<Double> finalWeight;
    public SexSaveMap(ArrayList<Double> finalWeight){
        this.finalWeight  = finalWeight;
    }
    public SexEntity map(String s) throws Exception {
        String [] datas = s.split(",");
        long userid = Long.valueOf(datas[0]);//用户id
        long ordernums = Long.valueOf(datas[1]);//订单次数
        long orderintenums = Long.valueOf(datas[2]);//订单频次
        long manClothes = Long.valueOf(datas[3]);;//浏览男装
        long chidrenClothes = Long.valueOf(datas[4]);;//浏览小孩
        long oldClothes = Long.valueOf(datas[5]);;//浏览老人
        long womenClothes = Long.valueOf(datas[6]);;//浏览女士
        double ordermountavg = Double.valueOf(datas[7]);;//订单平均金额
        long productscannums = Long.valueOf(datas[8]);;//浏览商品频次
        int label = Integer.valueOf(datas[2]);;//0 女 1 男
        ArrayList<String> dats = new ArrayList<>();
        dats.add(userid+"");
        dats.add(ordernums+"");
        dats.add(orderintenums+"");
        dats.add(manClothes+"");
        dats.add(chidrenClothes+"");
        dats.add(oldClothes+"");
        dats.add(womenClothes+"");
        dats.add(ordermountavg+"");
        dats.add(productscannums+"");
        String resultflag = Logistic.classifyVector(dats,finalWeight);
        String sex = resultflag=="0"?"女":"男";
        String tablename = "userinfo";
        String rowkey = userid+"";
        String famliyname="info";
        String colum = "sexinfo";
        HbaseUtils2.putdata(tablename,rowkey,famliyname,colum,sex);
        SexEntity sexEntity = new SexEntity();
        sexEntity.setUserid(userid);
        sexEntity.setSex(sex);
        long timeinfo = DateUtils.getCurrentWeekStart(System.currentTimeMillis());
        String groupField = "sexfiled=="+timeinfo+"=="+sex;
        sexEntity.setGroupField(groupField);
        return sexEntity;
    }
}


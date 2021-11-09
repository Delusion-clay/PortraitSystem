package com.it.userportrait.map;

import com.it.userportrait.analy.SexEntity;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-07-28 0:16
 */
public class SexMap implements MapFunction<String, SexEntity> {

    @Override
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
        Random random = new Random();
        String groupField = "sex=="+random.nextInt(100);
        SexEntity sexEntity = new SexEntity();
        sexEntity.setUserid(userid);
        sexEntity.setUserid(userid);
        sexEntity.setOrdernums(ordernums);
        sexEntity.setOrderintenums(orderintenums);
        sexEntity.setManClothes(manClothes);
        sexEntity.setChidrenClothes(chidrenClothes);
        sexEntity.setOldClothes(oldClothes);
        sexEntity.setWomenClothes(womenClothes);
        sexEntity.setOrdermountavg(ordermountavg);
        sexEntity.setProductscannums(productscannums);
        sexEntity.setLabel(label);
        sexEntity.setGroupField(groupField);
        return sexEntity;
    }
}


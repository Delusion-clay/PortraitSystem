package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.Point;
import com.it.userportrait.analy.UserGroupEntity;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.DistanceCompute;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

public class UserFeatureFinalGroupMap implements MapFunction<UserGroupEntity, UserGroupEntity> {
    private DistanceCompute disC = new DistanceCompute();
    private ArrayList<Point> pointArrayList;
    public UserFeatureFinalGroupMap( ArrayList<Point> pointArrayList){
        this.pointArrayList = pointArrayList;
    }
    @Override
    public UserGroupEntity map(UserGroupEntity userGroupEntity) throws Exception {
        long userid = userGroupEntity.getUserid();
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
        Point pointself = new Point(Integer.valueOf(userid+""),arraylFloat);
        float min_dis = Integer.MAX_VALUE;
        for (Point point : pointArrayList) {
            float tmp_dis = (float) Math.min(disC.getEuclideanDis(pointself, point), min_dis);
            if (tmp_dis != min_dis) {
                min_dis = tmp_dis;
                pointself.setClusterId(point.getId());
                pointself.setDist(min_dis);
                pointself.setPointCenter(point);
            }
        }
        String tableName =  "userinfo";
        String famliyname = "info";
        String cloumn = "groupinfo";
        String rowkeykey = userid+"";
        Point center = pointself.getPointCenter();
        String cneterInfo = JSONObject.toJSONString(center);
        HbaseUtils2.putdata(tableName,rowkeykey,famliyname,cloumn, cneterInfo);

        UserGroupEntity userGroupEntityresult = new UserGroupEntity();
        userGroupEntityresult.setCenterPoint(pointself.getPointCenter());
        long timeinfo = DateUtils.getCurrentMonthStart(System.currentTimeMillis());
        String groupField = "groupFenBu=="+timeinfo+"=="+pointself.getPointCenter().getId();
        userGroupEntity.setGroupField(groupField);
        userGroupEntity.setNumbers(1l);
        return userGroupEntity;
    }
}

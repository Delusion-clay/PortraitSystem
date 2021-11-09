package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.ConsumeEntity;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ConsumeUserAnlayMap implements  FlatMapFunction<ConsumeEntity, ConsumeEntity> {


    /**
     * 高消费   >1000 订单金额 4次  或者>200 次数15 次每月 >1000  1  > 200  2 < 200 3
     * 中等消费  >200 次数<15 次每月 >1000 小于4次
     * 低消费  <200 次数<15
     * @return
     * @throws Exception
     */
    @Override
    public void flatMap(ConsumeEntity consumeEntity, Collector<ConsumeEntity> collector) throws Exception {
        long userid = consumeEntity.getUserid();
        long numbers = consumeEntity.getNumbers();
        long timeinfo = consumeEntity.getTimeinfo();
        String consumeFlag = consumeEntity.getConsumeFlag();
        String tablename = "userinfoConsume";
        String rowkey = userid+"=="+timeinfo;
        String famliyname="info";
        String colum1 = "consumehighnums";
        String colum2 = "consumemiddlenums";
        String colum3 = "consumelownums";
        String columhighflag = "columhighflag";
        String colummiddleflag = "colummiddleflag";
        String columlowflag = "columlowflag";
        /**
         * 高消费   >1000 订单金额 4次  或者>200 次数15 次每月 >1000  1  > 200  2 < 200 3
         *      * 中等消费  >200 次数<15 次每月 >1000 小于4次
         *      * 低消费  <200 次数<15
         */
        String flag = "";
        List<ConsumeEntity> reusltList = new ArrayList<ConsumeEntity>();
        if(consumeFlag.equals("1")){
            String flag1 = HbaseUtils2.getdata(tablename,rowkey,famliyname,colum1);
            long pre = flag1.equals("")?0l:Long.valueOf(flag1);
            if(numbers+pre>4){
                flag = "high";
                HbaseUtils2.putdata(tablename,rowkey,famliyname,columhighflag,"1");
                reusltList = highProcess(tablename,rowkey,famliyname,colummiddleflag,columlowflag);
                HbaseUtils2.putdata(tablename,rowkey,famliyname,columhighflag,"1");
            }else if(numbers+pre>0 &&numbers+pre<4){
                flag = "middle";
                HbaseUtils2.putdata(tablename,rowkey,famliyname,colummiddleflag,"1");
                reusltList = middleProcess(tablename,rowkey,famliyname,columlowflag);
            }
            HbaseUtils2.putdata(tablename,rowkey,famliyname,colum1,numbers+pre+"");
        }else if(consumeFlag.equals("2")){
            String flag2 = HbaseUtils2.getdata(tablename,rowkey,famliyname,colum2);
            long pre = flag2.equals("")?0l:Long.valueOf(flag2);
            if(numbers+pre>15){
                flag = "high";
                HbaseUtils2.putdata(tablename,rowkey,famliyname,columhighflag,"1");
                reusltList =  highProcess(tablename,rowkey,famliyname,colummiddleflag,columlowflag);
            }else  if(numbers<15){
                flag = "middle";
                HbaseUtils2.putdata(tablename,rowkey,famliyname,colummiddleflag,"1");
                reusltList = middleProcess(tablename,rowkey,famliyname,columlowflag);
            }
            HbaseUtils2.putdata(tablename,rowkey,famliyname,colum2,numbers+pre+"");
        }else if(consumeFlag.equals("3")){
            String flag3 = HbaseUtils2.getdata(tablename,rowkey,famliyname,colum3);
            long pre = flag3.equals("")?0l:Long.valueOf(flag3);
            if(numbers+pre<15){
                flag = "low";
                HbaseUtils2.putdata(tablename,rowkey,famliyname,columlowflag,"1");
            }
            HbaseUtils2.putdata(tablename,rowkey,famliyname,colum3,numbers+pre+"");
        }

        ConsumeEntity consumeEntityResult = new ConsumeEntity();
        consumeEntityResult.setTimeinfo(timeinfo);
        consumeEntityResult.setNumbers(1l);
        consumeEntityResult.setConsumeFlag(flag);
        String groupField = "consumerFenBu=="+timeinfo+"=="+flag;
        consumeEntityResult.setGroupField(groupField);
        collector.collect(consumeEntity);
        for(ConsumeEntity consumeEntity1:reusltList){
            consumeEntity1.setTimeinfo(timeinfo);
            String groupField1 = "consumerFenBu=="+timeinfo+"=="+consumeEntity1.getConsumeFlag();
            consumeEntity1.setGroupField(groupField1);
            collector.collect(consumeEntity1);
        }
    }

    private List<ConsumeEntity> highProcess(String tablename, String rowkey, String famliyname, String colummiddleflag, String columlowflag) throws Exception {
        String middleflag = HbaseUtils2.getdata(tablename,rowkey,famliyname,colummiddleflag);
        String lowflag = HbaseUtils2.getdata(tablename,rowkey,famliyname,columlowflag);
        List<ConsumeEntity> reusltList = new ArrayList<ConsumeEntity>();
        if(StringUtils.isNotBlank(middleflag)&&middleflag.equals("1")){
            HbaseUtils2.putdata(tablename,rowkey,famliyname,colummiddleflag,"0");
            ConsumeEntity consumeEntityResult = new ConsumeEntity();
            consumeEntityResult.setNumbers(-1l);
            consumeEntityResult.setConsumeFlag("middle");
            reusltList.add(consumeEntityResult);
        }
        if(StringUtils.isNotBlank(lowflag)&&lowflag.equals("1")){
            HbaseUtils2.putdata(tablename,rowkey,famliyname,columlowflag,"0");
            ConsumeEntity consumeEntityResult = new ConsumeEntity();
            consumeEntityResult.setNumbers(-1l);
            consumeEntityResult.setConsumeFlag("low");
            reusltList.add(consumeEntityResult);
        }
        return reusltList;
    }

    private List<ConsumeEntity> middleProcess(String tablename,String rowkey,String famliyname,String columlowflag) throws Exception {
        String lowflag = HbaseUtils2.getdata(tablename,rowkey,famliyname,columlowflag);
        List<ConsumeEntity> reusltList = new ArrayList<ConsumeEntity>();
        if(StringUtils.isNotBlank(lowflag)&&lowflag.equals("1")){
            HbaseUtils2.putdata(tablename,rowkey,famliyname,columlowflag,"0");
            ConsumeEntity consumeEntityResult = new ConsumeEntity();
            consumeEntityResult.setNumbers(-1l);
            consumeEntityResult.setConsumeFlag("low");
            reusltList.add(consumeEntityResult);
        }
        return reusltList;
    }



}

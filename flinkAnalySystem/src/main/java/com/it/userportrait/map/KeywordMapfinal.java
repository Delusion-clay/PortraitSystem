package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.KeywordEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.HbaseUtils2;
import com.it.userportrait.util.MapUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

import java.util.*;


public class KeywordMapfinal implements FlatMapFunction<KeywordEntity, KeywordEntity> {

    private long totaldoucments = 0l;
    private long words;
    private String timeinfoString;
    public KeywordMapfinal(long totaldoucments, long words,String timeinfoString){
        this.totaldoucments = totaldoucments;
        this.words =words;
        this.timeinfoString = timeinfoString;

    }

    @Override
    public void flatMap(KeywordEntity keywordEntity, Collector<KeywordEntity> collector) throws Exception {
        Map<String,Double> tfidfmap = new HashMap<String,Double>();
        String documentid = keywordEntity.getDocumentid();
        Map<String,Double> tfmap = keywordEntity.getTfmap();
        Set<Map.Entry<String,Double>> set = tfmap.entrySet();
        String tablename = "tfidfdata";
        String rowkey="word";
        String famliyname="baseinfo";
        String colum="idfcount";
        for(Map.Entry<String,Double> entry:set){
            String word = entry.getKey();
            Double value = entry.getValue();
            String data = HbaseUtils2.getdata(tablename,rowkey,famliyname,colum);
            long viewcount = Long.valueOf(data);
            Double idf = Math.log(totaldoucments/viewcount+1);
            Double tfidf = value*idf;
            tfidfmap.put(word,tfidf);
        }
        LinkedHashMap<String,Double> resultfinal = MapUtils.sortMapByValue(tfidfmap);
        Set<Map.Entry<String,Double>> entryset = resultfinal.entrySet();
        List<String> finalword = new ArrayList<String>();
        int count =1;
        for(Map.Entry<String,Double> mapentry:entryset){
            finalword.add(mapentry.getKey());
            count++;
            if(count>words){
                break;
            }
        }
        KeywordEntity keywordEntityfinal = new KeywordEntity();
        Random random = new Random();

        String groupField = "";
        keywordEntityfinal.setTop1(finalword.get(0));
        keywordEntityfinal.setTimeinfoString(timeinfoString);
        keywordEntityfinal.setNumbers(1l);
        groupField = "keywordFenbuTop1=="+random.nextInt(10)+timeinfoString;
        keywordEntityfinal.setGroupField(groupField);
        collector.collect(keywordEntityfinal);
        keywordEntityfinal = new KeywordEntity();
        keywordEntityfinal.setTop2(finalword.get(1));
        keywordEntityfinal.setTimeinfoString(timeinfoString);
        keywordEntityfinal.setNumbers(1l);
        groupField = "keywordFenbuTop2=="+random.nextInt(10)+timeinfoString;
        keywordEntityfinal.setGroupField(groupField);
        collector.collect(keywordEntityfinal);
        keywordEntityfinal = new KeywordEntity();
        keywordEntityfinal.setTop3(finalword.get(2));
        keywordEntityfinal.setTimeinfoString(timeinfoString);
        keywordEntityfinal.setNumbers(1l);
        groupField = "keywordFenbuTop3=="+random.nextInt(10)+timeinfoString;
        keywordEntityfinal.setGroupField(groupField);
        collector.collect(keywordEntityfinal);

        keywordEntityfinal.setDocumentid(documentid);
        keywordEntityfinal.setFinalword(finalword);
        String finalwordString = JSONObject.toJSONString(finalword);
        String tablenamenew = "userkeywordinfo";
        Map<String, String> dataMap = new HashMap<String, String>();
        dataMap.put("userid",documentid);
        dataMap.put("timeinfoString",timeinfoString);
        dataMap.put("finalwordString",finalwordString);
        Set<String> intFieldSet = new HashSet<String>();
        ClickHouseUtils.insertData(tablenamenew,dataMap,intFieldSet);

    }
}

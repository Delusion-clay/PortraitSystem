package com.it.userportrait.analy;

import com.it.userportrait.map.Keword2AnalyMap;
import com.it.userportrait.map.KeywordMap;
import com.it.userportrait.map.KeywordMapfinal;
import com.it.userportrait.reduce.Keyword2AnlayReduce;
import com.it.userportrait.reduce.KeywordAnlayReduce;
import com.it.userportrait.reduce.KeywordFinalAnlayReduce;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.*;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-09-01 18:32
 */
public class KeywordYearAnaly {
    public static void main(String[] args) {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> fileSource =  env.readTextFile("hdfs://hadoop-101://orderinfoyear2.file");

        DataSet<KeywordEntity> map  = fileSource.map(new KeywordMap());
        DataSet<KeywordEntity> reduce  = map.groupBy("groupField").reduce(new KeywordAnlayReduce());
        DataSet<KeywordEntity> reduce2 = reduce.map(new Keword2AnalyMap()).groupBy("groupField").reduce(new Keyword2AnlayReduce());
        Long totaldoucment = 0l;
        try {
            totaldoucment = reduce2.collect().get(0).getTotaldocumet();
            long timeinfo = DateUtils.getCurrentMonthStart(System.currentTimeMillis());
            String timeinfoString = DateUtils.getDateString(timeinfo,"yyyy");
            DataSet<KeywordEntity> mapfinalresult = reduce2.flatMap(new KeywordMapfinal(totaldoucment,3,timeinfoString));
            DataSet<KeywordEntity> reducefinalresult = mapfinalresult.groupBy("groupField").reduce(new KeywordFinalAnlayReduce());
            List<KeywordEntity> result = reducefinalresult.collect();
            for(KeywordEntity keywordEntity:result){
                long numbers = keywordEntity.getNumbers();
                String timeinfoStr = keywordEntity.getTimeinfoString();
                String  top1String = keywordEntity.getTop1();
                String top2String = keywordEntity.getTop2();
                String top3String = keywordEntity.getTop2();

                String tablenamenew = "keywordfenbuYearinfo";
                Map<String, String> dataMap = new HashMap<String, String>();
                if(StringUtils.isNotBlank(top1String)){
                    dataMap.put("topn","top1("+top1String+")");
                }
                if(StringUtils.isNotBlank(top1String)){
                    dataMap.put("topn","top2("+top2String+")");
                }
                if(StringUtils.isNotBlank(top1String)){
                    dataMap.put("topn","top3("+top3String+")");
                }
                dataMap.put("timeinfoString",timeinfoStr);
                dataMap.put("numbers",numbers+"");
                Set<String> intFieldSet = new HashSet<String>();
                intFieldSet.add("numbers");
                ClickHouseUtils.insertData(tablenamenew,dataMap,intFieldSet);
            }
            env.execute("YoufanKeywordYearAnaly");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

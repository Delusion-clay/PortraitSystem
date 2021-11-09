package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.KeywordEntity;
import com.it.userportrait.util.HbaseUtils2;
import com.it.userportrait.util.IkUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

public class Keword2AnalyMap implements MapFunction<KeywordEntity, KeywordEntity> {


    @Override
    public KeywordEntity map(KeywordEntity keywordEntity) throws Exception {
        long userid = keywordEntity.getUserid();
        List<String> listdataFinal = new ArrayList<String>();
        List<String> titleList = keywordEntity.getProductTitile();
        for(String title:titleList){
            List<String> listdata = IkUtil.getIkWord(title);
            listdataFinal.addAll(listdata);
        }
        Map<String,Long> tfmap = new HashMap<String,Long>();

        Set<String> wordset = new HashSet<String>();
        for(String word:listdataFinal){
            Long pre = tfmap.get(word)==null?0l:tfmap.get(word);
            tfmap.put(word,pre+1);
            wordset.add(word);
        }

        KeywordEntity keywordEntity1 = new KeywordEntity();
        keywordEntity1.setDocumentid(userid+"==");
        keywordEntity1.setDatamap(tfmap);

        //计算总数
        long sum = 0l;
        Collection<Long> longset = tfmap.values();
        for(Long templong:longset){
            sum += templong;
        }

        Map<String,Double> tfmapfinal = new HashMap<String,Double>();
        Set<Map.Entry<String,Long>> entryset = tfmap.entrySet();
        for(Map.Entry<String,Long> entry:entryset){
            String word = entry.getKey();
            long count = entry.getValue();
            double tf = Double.valueOf(count)/Double.valueOf(sum);
            tfmapfinal.put(word,tf);
        }
        keywordEntity1.setTfmap(tfmapfinal);
        keywordEntity1.setTotaldocumet(1l);

        //create "tfidfdata,"baseinfo"
        for(String word:wordset){
            String tablename = "tfidfdata";
            String rowkey=word;
            String famliyname="baseinfo";
            String colum="idfcount";
            String data = HbaseUtils2.getdata(tablename,rowkey,famliyname,colum);
            Long pre = data==null?0l:Long.valueOf(data);
            Long total = pre+1;
            HbaseUtils2.putdata(tablename,rowkey,famliyname,colum,total+"");
        }

        return  keywordEntity1;
    }
}

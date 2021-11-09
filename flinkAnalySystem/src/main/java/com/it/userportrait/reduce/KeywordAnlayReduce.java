package com.it.userportrait.reduce;

import com.it.userportrait.analy.KeywordEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.List;

public class KeywordAnlayReduce implements ReduceFunction<KeywordEntity> {


    @Override
    public KeywordEntity reduce(KeywordEntity keywordEntity, KeywordEntity t1) throws Exception {
        long userid = keywordEntity.getUserid();
        String groupField = keywordEntity.getGroupField();
        List<String> title = keywordEntity.getProductTitile();
        List<String> title2 = t1.getProductTitile();
        KeywordEntity keywordEntity1 = new KeywordEntity();
        keywordEntity1.setGroupField(groupField);
        keywordEntity1.setUserid(userid);
        title.addAll(title2);
        keywordEntity1.setProductTitile(title);
        return keywordEntity1;
    }
}

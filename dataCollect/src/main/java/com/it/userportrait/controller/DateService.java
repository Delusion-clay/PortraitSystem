package com.it.userportrait.controller;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-07-26 23:46
 */
@RestController
@RequestMapping("dataService")
public class DateService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("data")
    public void reviceData(@RequestBody String data){
        JSONObject jsonObject = JSONObject.parseObject(data);
        System.out.println(jsonObject);
        String type = jsonObject.getString("diviceType");//0scan 1 collection 2 cart 3 Attention 4 adviser
        String topic = type.equals("0")?"scan":type.equals("1")?"collection":type.equals("2")?"cart":type.equals("3")?"Attention":"adviser";
        kafkaTemplate.send(topic,data);
    }
}

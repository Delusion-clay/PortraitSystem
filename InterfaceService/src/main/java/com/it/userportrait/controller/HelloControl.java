package com.it.userportrait.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloControl {

    @RequestMapping("hello")
    public String hello(String name){
       return name;
    }
}

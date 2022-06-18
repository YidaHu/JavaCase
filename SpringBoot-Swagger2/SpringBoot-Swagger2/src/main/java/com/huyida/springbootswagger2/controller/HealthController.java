package com.huyida.springbootswagger2.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @program: load-balancer
 * @description:
 * @author: huyida
 * @create: 2022-06-18 11:34
 **/
@Api(tags = "健康检查", value = "健康检查")
@RestController
@RequestMapping("/health")
public class HealthController {

    @ApiOperation(value = "健康检查", notes = "健康检查")
    @GetMapping("/check")
    public String check() {
        return "OK";
    }

}

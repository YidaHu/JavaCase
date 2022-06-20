package com.huyida.springbootswagger2.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @program: load-balancer
 * @description: Swagger配置
 * @author: huyida
 * @create: 2022-06-18 11:27
 **/
@Configuration
@EnableSwagger2
public class SwaggerConfiguration {

    @Bean //配置docket以配置Swagger具体参数
    public Docket buildDocket() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(buildApiInf())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.huyida.springbootswagger2.controller"))
                .paths(PathSelectors.any())
                .build();
    }

    private ApiInfo buildApiInf() {
        return new ApiInfoBuilder()
                .title("SpringBoot Swagger RESTful API文档")
                .contact(new Contact("huyida", "https://github.com/YidaHu/JavaCase", "huyidada@gmail.com"))
                .version("1.0")
                .build();
    }

}

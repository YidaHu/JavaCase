package com.huyida.consistenthash.test;

import lombok.*;

/**
 * @program: load-balancer
 * @description:
 * @author: huyida
 * @create: 2022-06-18 12:40
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class Node<T> {

    private String ip;

    private String name;

}

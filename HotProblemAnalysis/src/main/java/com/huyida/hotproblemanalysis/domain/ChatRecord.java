package com.huyida.hotproblemanalysis.domain;

import lombok.*;

/**
 * @program: HotProblemAnalysis
 * @description:
 * @author: huyida
 * @create: 2022-06-20 17:41
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class ChatRecord {

    private String question;

    private String answer;

    private Long timestamp;
}

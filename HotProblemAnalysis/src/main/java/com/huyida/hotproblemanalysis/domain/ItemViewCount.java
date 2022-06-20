package com.huyida.hotproblemanalysis.domain;

import lombok.*;

/**
 * @program: HotProblemAnalysis
 * @description:
 * @author: huyida
 * @create: 2022-06-20 19:13
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class ItemViewCount {
    private String itemId;
    private Long count;
    private Long windowEnd;

    public static ItemViewCount of(String itemId, Long end, Long count) {
        ItemViewCount itemBuyCount = new ItemViewCount();
        itemBuyCount.itemId = itemId;
        itemBuyCount.windowEnd = end;
        itemBuyCount.count = count;
        return itemBuyCount;
    }
}

package com.alibaba.dts.subscribe.positioner;

import com.alibaba.dts.subscribe.RdsSubscribeProperties;

public interface Positioner {

    /**
     * 保存偏移量
     */
    void save(RdsSubscribeProperties properties);

    /**
     * 加载配置
     *
     * @return
     */
    RdsSubscribeProperties loadRdsSubscribeProperties();
}

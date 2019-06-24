package com.alibaba.dts.subscribe.positioner;

import com.alibaba.dts.subscribe.RdsSubscribeProperties;

public class MemoryPositioner implements Positioner {

    private RdsSubscribeProperties rdsSubscribeProperties;

    public MemoryPositioner(RdsSubscribeProperties rdsSubscribeProperties) {
        this.rdsSubscribeProperties = rdsSubscribeProperties;
    }

    @Override
    public void save(RdsSubscribeProperties properties) {
        this.rdsSubscribeProperties = properties;
    }

    @Override
    public RdsSubscribeProperties loadRdsSubscribeProperties() {
        return this.rdsSubscribeProperties;
    }


}

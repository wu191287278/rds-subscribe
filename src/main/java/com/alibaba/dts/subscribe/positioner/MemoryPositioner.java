package com.alibaba.dts.subscribe.positioner;

import com.alibaba.dts.subscribe.RdsSubscribeProperties;
import com.alibaba.dts.subscribe.positioner.Positioner;

public class MemoryPositioner implements Positioner {

    private RdsSubscribeProperties rdsSubscribeProperties;

    public MemoryPositioner(RdsSubscribeProperties rdsSubscribeProperties) {
        this.rdsSubscribeProperties = rdsSubscribeProperties;
    }

    @Override
    public void save(RdsSubscribeProperties properties) {
        this.rdsSubscribeProperties = properties;
        System.err.println(this.rdsSubscribeProperties);
    }

    @Override
    public RdsSubscribeProperties loadRdsSubscribeProperties() {
        return this.rdsSubscribeProperties;
    }


}

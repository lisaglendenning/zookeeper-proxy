package org.apache.zookeeper.proxy.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.apache.zookeeper.netty.server.NioServerBootstrapFactory;

import com.google.inject.Provides;

public class NioProxyBootstrapFactory extends NioServerBootstrapFactory {
    
    public static NioProxyBootstrapFactory get() {
        return new NioProxyBootstrapFactory();
    }
    
    protected NioProxyBootstrapFactory() {}
    
    @Override
    protected void configure() {
        super.configure();
    }

    @Provides
    public Bootstrap getBootstrap(
            Class<? extends Channel> channelType,
            EventLoopGroup group) {
        return newBootstrap(channelType, group);
    }
    
}
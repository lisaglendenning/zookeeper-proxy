package edu.uw.zookeeper.proxy.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import com.google.inject.Provides;

import edu.uw.zookeeper.netty.server.NioServerBootstrapFactory;

public class NioProxyBootstrapFactory extends NioServerBootstrapFactory {

    public static NioProxyBootstrapFactory get() {
        return new NioProxyBootstrapFactory();
    }

    protected NioProxyBootstrapFactory() {
    }

    @Provides
    public Bootstrap getBootstrap(Class<? extends Channel> channelType,
            EventLoopGroup group) {
        return newBootstrap(channelType, group);
    }
}

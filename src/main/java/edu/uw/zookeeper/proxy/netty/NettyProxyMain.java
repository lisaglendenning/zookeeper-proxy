package edu.uw.zookeeper.proxy.netty;

import java.util.List;


import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientConnectionGroup;
import edu.uw.zookeeper.netty.client.ChannelClientConnectionGroup;
import edu.uw.zookeeper.netty.client.ClientConnection;
import edu.uw.zookeeper.netty.server.ConfigurableChannelServerConnectionGroup;
import edu.uw.zookeeper.netty.server.ServerConnection;
import edu.uw.zookeeper.proxy.ProxyMain;
import edu.uw.zookeeper.server.ServerConnectionGroup;
import edu.uw.zookeeper.util.ServiceMonitor;

public class NettyProxyMain extends ProxyMain {

    public static void main(String[] args) throws Exception {
        NettyProxyMain main = get();
        main.apply(args);
    }

    public static NettyProxyMain get() {
        return new NettyProxyMain();
    }

    protected NettyProxyMain() {
    }

    @Override
    protected void configure() {
        super.configure();

        // server
        bind(ServerConnection.Factory.class).in(Singleton.class);
        bind(ConfigurableChannelServerConnectionGroup.class)
                .in(Singleton.class);

        // client
        bind(ClientConnection.Factory.class).in(Singleton.class);
        bind(ChannelClientConnectionGroup.class).in(Singleton.class);
    }

    @Override
    protected List<Module> modules() {
        List<Module> modules = super.modules();
        modules.add(NioProxyBootstrapFactory.get());
        return modules;
    }

    @Provides
    @Singleton
    protected ServerConnectionGroup getServerConnectionGroup(
            ConfigurableChannelServerConnectionGroup group,
            ServiceMonitor monitor) {
        monitor.add(group);
        return group;
    }

    @Provides
    @Singleton
    protected ClientConnectionGroup getClientConnectionGroup(
            ChannelClientConnectionGroup group, ServiceMonitor monitor) {
        monitor.add(group);
        return group;
    }
}

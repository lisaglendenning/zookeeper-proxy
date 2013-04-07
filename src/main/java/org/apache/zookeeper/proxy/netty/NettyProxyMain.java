package org.apache.zookeeper.proxy.netty;

import java.util.List;

import org.apache.zookeeper.client.ClientConnectionGroup;
import org.apache.zookeeper.netty.client.ChannelClientConnectionGroup;
import org.apache.zookeeper.netty.client.ClientConnection;
import org.apache.zookeeper.netty.client.NioBootstrapFactory;
import org.apache.zookeeper.netty.server.ChannelServerConnectionGroup;
import org.apache.zookeeper.netty.server.ConfigurableChannelServerConnectionGroup;
import org.apache.zookeeper.netty.server.NioServerBootstrapFactory;
import org.apache.zookeeper.netty.server.ServerConnection;
import org.apache.zookeeper.proxy.ProxyMain;
import org.apache.zookeeper.server.ServerConnectionGroup;
import org.apache.zookeeper.util.ServiceMonitor;

import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class NettyProxyMain extends ProxyMain {

    public static void main(String[] args) throws Exception {
        NettyProxyMain main = get();
        main.apply(args);
    }

    public static NettyProxyMain get() {
        return new NettyProxyMain();
    }

    protected NettyProxyMain() {}
    
    @Override
    protected void configure() {
        super.configure();
        
        // server
        bind(ServerConnection.Factory.class).in(Singleton.class);
        bind(ConfigurableChannelServerConnectionGroup.class).in(Singleton.class);
        
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

    @Provides @Singleton
    protected ServerConnectionGroup getServerConnectionGroup(ConfigurableChannelServerConnectionGroup group, ServiceMonitor monitor) {
        monitor.add(group);
        return group;
    }

    @Provides @Singleton
    protected ClientConnectionGroup getClientConnectionGroup(ChannelClientConnectionGroup group, ServiceMonitor monitor) {
        monitor.add(group);
        return group;
    }
}

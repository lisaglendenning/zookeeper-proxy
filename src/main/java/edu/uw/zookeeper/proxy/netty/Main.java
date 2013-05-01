package edu.uw.zookeeper.proxy.netty;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.netty.DaemonThreadFactory;
import edu.uw.zookeeper.netty.MonitoredEventLoopGroupFactory;
import edu.uw.zookeeper.netty.client.NioClientBootstrapFactory;
import edu.uw.zookeeper.netty.nio.NioEventLoopGroupFactory;
import edu.uw.zookeeper.netty.server.NioServerBootstrapFactory;
import edu.uw.zookeeper.proxy.ProxyMain;
import edu.uw.zookeeper.util.ConfigurableMain;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Singleton;

public class Main extends ProxyMain {

    public static void main(String[] args) {
        ConfigurableMain.main(args, ConfigurableMain.DefaultApplicationFactory.newInstance(Main.class));
    }

    protected final Factory<? extends ChannelClientConnectionFactory> clientConnectionFactory;
    protected final ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory> serverConnectionFactory;
    
    public Main(Configuration configuration) {
        super(configuration);
        // shared eventloopgroup
        ThreadFactory threads = DaemonThreadFactory.getInstance().get(threadFactory().get());
        Singleton<? extends EventLoopGroup> groupFactory = MonitoredEventLoopGroupFactory.newInstance(
                NioEventLoopGroupFactory.getInstance(),
                serviceMonitor()).get(threads);

        ParameterizedFactory<Channel, ChannelConnection> connectionBuilder = ChannelConnection.ConnectionBuilder.newInstance(publisherFactory());
        
        // client
        Factory<Bootstrap> bootstrapFactory = NioClientBootstrapFactory.newInstance(groupFactory);        
        this.clientConnectionFactory = 
                ChannelClientConnectionFactory.ClientFactoryBuilder.newInstance(publisherFactory(), connectionBuilder, bootstrapFactory);

        // server
        ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(threadFactory(), serviceMonitor()));
        this.serverConnectionFactory = ChannelServerConnectionFactory.ParameterizedServerFactoryBuilder.newInstance(publisherFactory(), connectionBuilder, serverBootstrapFactory);
    }

    @Override
    protected Factory<? extends ChannelClientConnectionFactory> clientConnectionFactory() {
        return clientConnectionFactory;
    }

    @Override
    protected ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory> serverConnectionFactory() {
        return serverConnectionFactory;
    }
}

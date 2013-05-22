package edu.uw.zookeeper.proxy.netty;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.netty.DaemonThreadFactory;
import edu.uw.zookeeper.netty.MonitoredEventLoopGroupFactory;
import edu.uw.zookeeper.netty.client.NioClientBootstrapFactory;
import edu.uw.zookeeper.netty.nio.NioEventLoopGroupFactory;
import edu.uw.zookeeper.netty.server.NioServerBootstrapFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Singleton;

public class NettyModule {

    public static NettyModule newInstance(RuntimeModule main) {
        return new NettyModule(main);
    }
    
    public static enum EventLoopGroupFactory implements ParameterizedFactory<RuntimeModule, Singleton<? extends EventLoopGroup>> {
        INSTANCE;
        
        @Override
        public Singleton<? extends EventLoopGroup> get(RuntimeModule main) {
            ThreadFactory threads = DaemonThreadFactory.getInstance().get(main.threadFactory().get());
            return MonitoredEventLoopGroupFactory.newInstance(
                    NioEventLoopGroupFactory.DEFAULT,
                    main.serviceMonitor()).get(threads);
        }
    }
    
    protected final Singleton<? extends EventLoopGroup> groupFactory;
    protected final Factory<? extends ChannelClientConnectionFactory> clientConnectionFactory;
    protected final ParameterizedFactory<SocketAddress, ? extends ChannelServerConnectionFactory> serverConnectionFactory;
    
    public NettyModule(RuntimeModule runtime) {
        // shared eventloopgroup
        this.groupFactory = EventLoopGroupFactory.INSTANCE.get(runtime);
        
        ParameterizedFactory<Channel, ChannelConnection> connectionBuilder = 
                ChannelConnection.PerConnectionPublisherFactory.newInstance(runtime.publisherFactory());
        
        // client
        Factory<Bootstrap> bootstrapFactory = NioClientBootstrapFactory.newInstance(groupFactory);        
        this.clientConnectionFactory = 
                ChannelClientConnectionFactory.ClientFactoryBuilder.newInstance(runtime.publisherFactory(), connectionBuilder, bootstrapFactory);

        // server
        ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(groupFactory));
        this.serverConnectionFactory = ChannelServerConnectionFactory.ParameterizedServerFactoryBuilder.newInstance(
                runtime.publisherFactory(), connectionBuilder, serverBootstrapFactory);
    }

    public Factory<? extends ChannelClientConnectionFactory> clientConnectionFactory() {
        return clientConnectionFactory;
    }

    public ParameterizedFactory<SocketAddress, ? extends ChannelServerConnectionFactory> serverConnectionFactory() {
        return serverConnectionFactory;
    }
}

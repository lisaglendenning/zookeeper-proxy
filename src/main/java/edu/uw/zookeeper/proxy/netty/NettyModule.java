package edu.uw.zookeeper.proxy.netty;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.Singleton;
import edu.uw.zookeeper.netty.DaemonThreadFactory;
import edu.uw.zookeeper.netty.EventLoopGroupService;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.netty.client.NioClientBootstrapFactory;
import edu.uw.zookeeper.netty.nio.NioEventLoopGroupFactory;
import edu.uw.zookeeper.netty.server.NioServerBootstrapFactory;
import edu.uw.zookeeper.netty.server.NettyServerModule;

public class NettyModule {

    public static NettyModule newInstance(RuntimeModule main) {
        return new NettyModule(main);
    }
    
    public static enum EventLoopGroupFactory implements ParameterizedFactory<RuntimeModule, Singleton<? extends EventLoopGroup>> {
        INSTANCE;
        
        @Override
        public Singleton<? extends EventLoopGroup> get(RuntimeModule main) {
            ThreadFactory threads = DaemonThreadFactory.getInstance().get(main.threadFactory().get());
            return EventLoopGroupService.factory(
                    NioEventLoopGroupFactory.DEFAULT,
                    main.serviceMonitor()).get(threads);
        }
    }
    
    protected final Singleton<? extends EventLoopGroup> groupFactory;
    protected final NettyClientModule nettyClient;
    protected final NettyServerModule nettyServer;    
    
    public NettyModule(RuntimeModule runtime) {
        final Factory<Publisher> publisherFactory = runtime.publisherFactory();
        
        // shared eventloopgroup
        this.groupFactory = EventLoopGroupFactory.INSTANCE.get(runtime);
        
        // client
        final Factory<Bootstrap> bootstrapFactory = 
                NioClientBootstrapFactory.newInstance(groupFactory);        
        this.nettyClient = 
                NettyClientModule.newInstance(publisherFactory, bootstrapFactory);

        // server
        final ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(groupFactory));
        this.nettyServer = 
                NettyServerModule.newInstance(publisherFactory, serverBootstrapFactory);
    }

    public NettyClientModule clients() {
        return nettyClient;
    }

    public NettyServerModule servers() {
        return nettyServer;
    }
}

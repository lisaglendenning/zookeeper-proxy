package edu.uw.zookeeper.proxy;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.bus.SyncBusConfiguration;
import net.engio.mbassy.bus.SyncMessageBus;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.netty.DaemonThreadFactory;
import edu.uw.zookeeper.netty.EventLoopGroupService;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.netty.client.NioClientBootstrapFactory;
import edu.uw.zookeeper.netty.NioEventLoopGroupFactory;
import edu.uw.zookeeper.netty.server.NioServerBootstrapFactory;
import edu.uw.zookeeper.netty.server.NettyServerModule;

public class NettyModule {

    public static NettyModule newInstance(RuntimeModule runtime) {
        return new NettyModule(runtime);
    }
    
    public static enum EventLoopGroupFactory implements ParameterizedFactory<RuntimeModule, Reference<? extends EventLoopGroup>> {
        INSTANCE;
        
        @Override
        public Reference<? extends EventLoopGroup> get(RuntimeModule main) {
            ThreadFactory threads = DaemonThreadFactory.getInstance().get(main.getThreadFactory().get());
            return EventLoopGroupService.factory(
                    NioEventLoopGroupFactory.DEFAULT,
                    main.getServiceMonitor()).get(threads);
        }
    }

    public static Factory<SyncMessageBus<Object>> syncMessageBus() {
        return new Factory<SyncMessageBus<Object>>() {
            @SuppressWarnings("rawtypes")
            @Override
            public SyncMessageBus<Object> get() {
                return new SyncMessageBus<Object>(new SyncBusConfiguration());
            }
        };
    }

    protected final Reference<? extends EventLoopGroup> groupFactory;
    protected final NettyClientModule nettyClient;
    protected final NettyServerModule nettyServer;    
    
    public NettyModule(RuntimeModule runtime) {
        Factory<? extends PubSubSupport<Object>> publisherFactory = syncMessageBus();
        
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

package edu.uw.zookeeper.proxy.netty;

import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.Connection.CodecFactory;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.netty.DaemonThreadFactory;
import edu.uw.zookeeper.netty.EventLoopGroupService;
import edu.uw.zookeeper.netty.client.ClientModule;
import edu.uw.zookeeper.netty.client.NioClientBootstrapFactory;
import edu.uw.zookeeper.netty.nio.NioEventLoopGroupFactory;
import edu.uw.zookeeper.netty.server.NioServerBootstrapFactory;
import edu.uw.zookeeper.netty.server.ServerModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerCodecConnection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;
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
            return EventLoopGroupService.factory(
                    NioEventLoopGroupFactory.DEFAULT,
                    main.serviceMonitor()).get(threads);
        }
    }
    
    protected final Singleton<? extends EventLoopGroup> groupFactory;
    protected final ParameterizedFactory<CodecFactory<Message.ClientSessionMessage, Message.ServerSessionMessage, PingingClientCodecConnection>, Factory<ChannelClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection>>> clientConnectionFactory;
    protected final ParameterizedFactory<Connection.CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection>, ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>>> serverConnectionFactory;
    
    public NettyModule(RuntimeModule runtime) {
        final Factory<Publisher> publisherFactory = runtime.publisherFactory();
        
        // shared eventloopgroup
        this.groupFactory = EventLoopGroupFactory.INSTANCE.get(runtime);
        
        // client
        final Factory<Bootstrap> bootstrapFactory = 
                NioClientBootstrapFactory.newInstance(groupFactory);        
        this.clientConnectionFactory = 
                ClientModule.factory(publisherFactory, bootstrapFactory);

        // server
        final ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(groupFactory));
        this.serverConnectionFactory = 
                ServerModule.factory(publisherFactory, serverBootstrapFactory);
    }

    public Factory<ChannelClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection>> clientConnectionFactory(CodecFactory<Message.ClientSessionMessage, Message.ServerSessionMessage, PingingClientCodecConnection> codecFactory) {
        return clientConnectionFactory.get(codecFactory);
    }

    public ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<Message.ServerMessage, ServerCodecConnection>> serverConnectionFactory(Connection.CodecFactory<Message.ServerMessage, Message.ClientMessage, ServerCodecConnection> codecFactory) {
        return serverConnectionFactory.get(codecFactory);
    }
}

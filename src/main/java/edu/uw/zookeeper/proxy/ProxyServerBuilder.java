package edu.uw.zookeeper.proxy;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.clients.ConnectionClientExecutorsService;
import edu.uw.zookeeper.common.*;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.MessageClientExecutor;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.server.ConnectionServerExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.server.ServerConnectionFactoryBuilder;

public class ProxyServerBuilder extends ConnectionServerExecutorsService.Builder {

    public static ProxyServerBuilder defaults() {
        return new ProxyServerBuilder();
    }
    
    @Configurable(arg="servers", key="servers", value="127.0.0.1:2081", help="address:port,...")
    public static class ConfigurableEnsembleView extends edu.uw.zookeeper.client.ConfigurableEnsembleView {

        public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
            return new ConfigurableEnsembleView().apply(configuration);
        }
    }

    public static class FromRequestFactory<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> implements DefaultsFactory<ConnectMessage.Request, ListenableFuture<MessageClientExecutor<C>>> {
    
        public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> FromRequestFactory<C> create(
                Factory<ListenableFuture<C>> connections,
                ScheduledExecutorService executor) {
            return new FromRequestFactory<C>(connections, executor);
        }

        protected final static Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();

        protected final Factory<ListenableFuture<C>> connections;
        protected final ScheduledExecutorService executor;
        
        public FromRequestFactory(
                Factory<ListenableFuture<C>> connections,
                ScheduledExecutorService executor) {
            this.connections = connections;
            this.executor = executor;
        }

        @Override
        public ListenableFuture<MessageClientExecutor<C>> get() {
            return get(ConnectMessage.Request.NewRequest.newInstance());
        }
        
        @Override
        public ListenableFuture<MessageClientExecutor<C>> get(ConnectMessage.Request request) {
            return Futures.transform(connections.get(), new Constructor(request), sameThreadExecutor);
        }
        
        protected class Constructor implements Function<C, MessageClientExecutor<C>> {

            protected final ConnectMessage.Request task;
            
            public Constructor(ConnectMessage.Request task) {
                this.task = task;
            }
            
            @Override
            public MessageClientExecutor<C> apply(C input) {
                return MessageClientExecutor.newInstance(
                        task, input, executor);
            }
        }
    }
    
    public static class ServerViewFactories implements ParameterizedFactory<ServerInetAddressView, ServerViewFactory<ConnectMessage.Request, ? extends MessageClientExecutor<?>>> {
    
        public static ServerViewFactories newInstance(
                ClientConnectionFactory<? extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> connections,
                ScheduledExecutorService executor) {
            return new ServerViewFactories(connections, executor);
        }
        
        protected final ClientConnectionFactory<? extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> connections;
        protected final ScheduledExecutorService executor;
        
        protected ServerViewFactories(
                ClientConnectionFactory<? extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> connections,
                ScheduledExecutorService executor) {
            this.connections = connections;
            this.executor = executor;
        }
    
        @Override
        public ServerViewFactory<ConnectMessage.Request, ? extends MessageClientExecutor<?>> get(ServerInetAddressView view) {
            return ServerViewFactory.newInstance(
                    view, 
                    FromRequestFactory.create(
                            FixedClientConnectionFactory.create(view.get(), connections),
                            executor), 
                    ZxidTracker.create());
        }
    }

    public static class ClientBuilder extends ConnectionClientExecutorsService.AbstractBuilder<ConnectionClientExecutorsService<Message.ClientRequest<?>, ConnectMessage.Request, MessageClientExecutor<?>>, ClientBuilder> {

        public static ClientBuilder defaults() {
            return new ClientBuilder(null, null, null, null);
        }
        
        protected ClientBuilder(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
                ConnectionClientExecutorsService<Message.ClientRequest<?>, ConnectMessage.Request, MessageClientExecutor<?>> clientExecutors,
                RuntimeModule runtime) {
            super(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
        }

        @Override
        protected ClientBuilder newInstance(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolCodecConnection<Message.ClientSession, ? extends ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>> clientConnectionFactory,
                ConnectionClientExecutorsService<Message.ClientRequest<?>, ConnectMessage.Request, MessageClientExecutor<?>> clientExecutors,
                RuntimeModule runtime) {
            return new ClientBuilder(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
        }

        @Override
        protected ConnectionClientExecutorsService<Message.ClientRequest<?>, ConnectMessage.Request, MessageClientExecutor<?>> getDefaultConnectionClientExecutorsService() {
            EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(getRuntimeModule().getConfiguration());
            final EnsembleViewFactory<? extends ServerViewFactory<ConnectMessage.Request, ? extends MessageClientExecutor<?>>> ensembleFactory = 
                    EnsembleViewFactory.random(
                        ensemble, 
                        ServerViewFactories.newInstance(
                                clientConnectionFactory, 
                                getRuntimeModule().getExecutors().get(ScheduledExecutorService.class)));
            ConnectionClientExecutorsService<Message.ClientRequest<?>, ConnectMessage.Request, MessageClientExecutor<?>> service =
                    ConnectionClientExecutorsService.newInstance(
                            new DefaultsFactory<ConnectMessage.Request, ListenableFuture<? extends MessageClientExecutor<?>>>() {
                                @Override
                                public ListenableFuture<? extends MessageClientExecutor<?>> get(ConnectMessage.Request value) {
                                    return ensembleFactory.get().get(value);
                                }
                                @Override
                                public ListenableFuture<? extends MessageClientExecutor<?>> get() {
                                    return ensembleFactory.get().get();
                                }
                            });
            return service;
        }
    }
    
    protected final Logger logger = LogManager.getLogger(getClass());
    protected final NettyModule netModule;
    protected final ClientBuilder clientBuilder;
    
    protected ProxyServerBuilder() {
        this(null, null, null, null, null, null, null, null);
    }

    protected ProxyServerBuilder(
            NettyModule netModule,
            ClientBuilder clientBuilder,
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor,
            ConnectionServerExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
            TimeValue timeOut,
            RuntimeModule runtime) {
        super(connectionBuilder, serverConnectionFactory, serverTaskExecutor,
                connectionExecutors, timeOut, runtime);
        this.netModule = netModule;
        this.clientBuilder = clientBuilder;
    }

    @Override
    public ProxyServerBuilder setRuntimeModule(RuntimeModule runtime) {
        if (this.runtime == runtime) {
            return this;
        } else {
            return newInstance(
                    netModule,
                    (clientBuilder == null) ? clientBuilder : clientBuilder.setRuntimeModule(runtime),
                    (connectionBuilder == null) ? connectionBuilder : connectionBuilder.setRuntimeModule(runtime), 
                    serverConnectionFactory, 
                    serverTaskExecutor, 
                    connectionExecutors, 
                    timeOut,
                    runtime);
        }
    }
    
    public NettyModule getNetModule() {
        return netModule;
    }
    
    public ProxyServerBuilder setNetModule(NettyModule netModule) {
        if (this.netModule == netModule) {
            return this;
        } else {
            return newInstance(
                    netModule,
                    clientBuilder,
                    connectionBuilder, 
                    serverConnectionFactory, 
                    serverTaskExecutor, 
                    connectionExecutors, 
                    timeOut,
                    runtime);
        }
    }
    
    public ClientBuilder getClientBuilder() {
        return clientBuilder;
    }

    public ProxyServerBuilder setClientBuilder(
            ClientBuilder clientBuilder) {
        if (this.clientBuilder == clientBuilder) {
            return this;
        } else {
            return newInstance(netModule, 
                    clientBuilder, 
                    connectionBuilder, 
                    serverConnectionFactory, 
                    serverTaskExecutor, 
                    connectionExecutors,
                    timeOut,
                    runtime);
        }
    }

    @Override
    public ProxyServerBuilder setDefaults() {
        if (netModule == null) {
            return setNetModule(getDefaultNetModule()).setDefaults();
        }
        if (clientBuilder == null) {
            return setClientBuilder(getDefaultClientBuilder()).setDefaults();
        }
        return (ProxyServerBuilder) super.setDefaults();
    }

    @Override
    protected List<Service> getServices() {
        List<Service> services = Lists.newLinkedList();
        for (Service e: getClientBuilder().build()) {
            services.add(e);
        }
        for (Service e: super.getServices()) {
            services.add(e);
        }
        return services;
    }
    
    @Override
    protected ProxyServerBuilder newInstance(
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor,
            ConnectionServerExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
            TimeValue timeOut,
            RuntimeModule runtime) {
        return newInstance(netModule, clientBuilder, connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, timeOut, runtime);
    }
    
    protected ProxyServerBuilder newInstance(
            NettyModule netModule,
            ClientBuilder clientBuilder,
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor,
            ConnectionServerExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
                    TimeValue timeOut,
            RuntimeModule runtime) {
        return new ProxyServerBuilder(netModule, clientBuilder, connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, timeOut, runtime);
    }

    protected NettyModule getDefaultNetModule() {
        return NettyModule.newInstance(getRuntimeModule());
    }

    protected ClientBuilder getDefaultClientBuilder() {
        return ClientBuilder.defaults().setConnectionBuilder(
                ClientConnectionFactoryBuilder.defaults()
                    .setTimeOut(getTimeOut())
                    .setClientModule(netModule.clients())
                    .setConnectionFactory(ProtocolCodecConnection.<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>factory()))
                .setRuntimeModule(runtime).setDefaults();
    }

    @Override
    protected ServerTaskExecutor getDefaultServerTaskExecutor() {
        return ProxyServerTaskExecutor.newInstance(
                getRuntimeModule().getExecutors().get(ExecutorService.class),
                clientBuilder.getConnectionClientExecutors());
    }
    
    @Override
    protected ServerConnectionFactoryBuilder getDefaultServerConnectionFactoryBuilder() {
        return ServerConnectionFactoryBuilder.defaults()
                .setServerModule(netModule.servers())
                .setRuntimeModule(runtime)
                .setDefaults();
    }
    
    @Override
    protected TimeValue getDefaultTimeOut() {
        return getClientBuilder().getConnectionBuilder().getTimeOut();
    }
}

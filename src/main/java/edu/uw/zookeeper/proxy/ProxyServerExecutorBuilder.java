package edu.uw.zookeeper.proxy;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ConnectionClientExecutorsService;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.*;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.protocol.client.MessageClientExecutor;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.server.ProcessorTaskExecutor;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class ProxyServerExecutorBuilder extends ZooKeeperApplication.ForwardingBuilder<SimpleServerExecutor<ProxySessionExecutor>, ProxyServerExecutorBuilder.ClientBuilder, ProxyServerExecutorBuilder> {

    public static ProxyServerExecutorBuilder defaults() {
        return new ProxyServerExecutorBuilder(null, ClientBuilder.defaults());
    }
    
    @Configurable(arg="servers", key="servers", value="127.0.0.1:2081", help="address:port,...")
    public static abstract class EnsembleViewConfiguration {

        public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
            Configurable configurable = getConfigurable();
            String value = 
                    configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path())
                        .getString(configurable.key());
            return ServerInetAddressView.ensembleFromString(value);
        }

        public static Configurable getConfigurable() {
            return EnsembleViewConfiguration.class.getAnnotation(Configurable.class);
        }
        
        public static Configuration set(Configuration configuration, EnsembleView<ServerInetAddressView> value) {
            Configurable configurable = getConfigurable();
            return configuration.withConfig(ConfigFactory.parseMap(ImmutableMap.<String,Object>builder().put(ConfigUtil.joinPath(configurable.path(), configurable.key()), EnsembleView.toString(value)).build()));
        }
        
        protected EnsembleViewConfiguration() {}
    }

    public static class FromRequestFactory<C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> implements DefaultsFactory<ConnectMessage.Request, ListenableFuture<MessageClientExecutor<C>>> {
    
        public static <C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> FromRequestFactory<C> create(
                Factory<? extends ListenableFuture<? extends C>> connections,
                ScheduledExecutorService executor) {
            return new FromRequestFactory<C>(connections, executor);
        }

        protected final Factory<? extends ListenableFuture<? extends C>> connections;
        protected final ScheduledExecutorService executor;
        
        public FromRequestFactory(
                Factory<? extends ListenableFuture<? extends C>> connections,
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
            return Futures.transform(connections.get(), new Constructor(request), SameThreadExecutor.getInstance());
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
                ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections,
                ScheduledExecutorService executor) {
            return new ServerViewFactories(connections, executor);
        }
        
        protected final ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections;
        protected final ScheduledExecutorService executor;
        
        protected ServerViewFactories(
                ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections,
                ScheduledExecutorService executor) {
            this.connections = connections;
            this.executor = executor;
        }
    
        @Override
        public ServerViewFactory<ConnectMessage.Request, ? extends MessageClientExecutor<?>> get(ServerInetAddressView view) {
            return ServerViewFactory.create(
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
                ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> clientConnectionFactory,
                ConnectionClientExecutorsService<Message.ClientRequest<?>, ConnectMessage.Request, MessageClientExecutor<?>> clientExecutors,
                RuntimeModule runtime) {
            super(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
        }

        @Override
        protected ClientBuilder newInstance(
                ClientConnectionFactoryBuilder connectionBuilder,
                ClientConnectionFactory<? extends ProtocolConnection<Message.ClientSession, Message.ServerSession,?,?,?>> clientConnectionFactory,
                ConnectionClientExecutorsService<Message.ClientRequest<?>, ConnectMessage.Request, MessageClientExecutor<?>> clientExecutors,
                RuntimeModule runtime) {
            return new ClientBuilder(connectionBuilder, clientConnectionFactory, clientExecutors, runtime);
        }

        @Override
        protected ConnectionClientExecutorsService<Message.ClientRequest<?>, ConnectMessage.Request, MessageClientExecutor<?>> getDefaultConnectionClientExecutorsService() {
            EnsembleView<ServerInetAddressView> ensemble = EnsembleViewConfiguration.get(getRuntimeModule().getConfiguration());
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
    
    protected ProxyServerExecutorBuilder(
            NettyModule netModule,
            ClientBuilder clientBuilder) {
        super(clientBuilder);
        this.netModule = netModule;
    }

    public NettyModule getNetModule() {
        return netModule;
    }
    
    public ProxyServerExecutorBuilder setNetModule(NettyModule netModule) {
        if (this.netModule == netModule) {
            return this;
        } else {
            return newInstance(
                    netModule,
                    delegate);
        }
    }
    
    public ClientBuilder getClientBuilder() {
        return delegate;
    }

    public ProxyServerExecutorBuilder setClientBuilder(
            ClientBuilder clientBuilder) {
        if (this.delegate == clientBuilder) {
            return this;
        } else {
            return newInstance(netModule, 
                    clientBuilder);
        }
    }

    @Override
    public ProxyServerExecutorBuilder setDefaults() {
        if (getNetModule() == null) {
            return setNetModule(getDefaultNetModule()).setDefaults();
        }
        ClientBuilder clientBuilder = getDefaultClientBuilder();
        if (getClientBuilder() != clientBuilder) {
            return setClientBuilder(clientBuilder).setDefaults();
        }
        return this;
    }

    @Override
    protected ProxyServerExecutorBuilder newInstance(
            ClientBuilder clientBuilder) {
        return newInstance(netModule, clientBuilder);
    }
    
    protected ProxyServerExecutorBuilder newInstance(
            NettyModule netModule,
            ClientBuilder clientBuilder) {
        return new ProxyServerExecutorBuilder(netModule, clientBuilder);
    }

    protected NettyModule getDefaultNetModule() {
        return NettyModule.newInstance(getRuntimeModule());
    }

    protected ClientBuilder getDefaultClientBuilder() {
        ClientBuilder builder = getClientBuilder();
        if (builder.getConnectionBuilder() == null) {
            builder = builder.setConnectionBuilder(
                    ClientConnectionFactoryBuilder.defaults()
                        .setClientModule(getNetModule().clients())
                        .setConnectionFactory(
                                new ParameterizedFactory<CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession,Message.ClientSession,Message.ServerSession>,?>, ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?>>() {
                                    @Override
                                    public ClientProtocolConnection<Message.ClientSession, Message.ServerSession,?,?> get(CodecConnection<Message.ClientSession, Message.ServerSession, ProtocolCodec<Message.ClientSession,Message.ServerSession,Message.ClientSession,Message.ServerSession>,?> value) {
                                        return ClientProtocolConnection.newInstance(value);
                                    }
                                }));
        }
        return builder.setDefaults();
    }

    @Override
    protected SimpleServerExecutor<ProxySessionExecutor> doBuild() {
        ConcurrentMap<Long, ProxySessionExecutor> sessions = new MapMaker().makeMap();
        ProxyConnectExecutor connectExecutor = ProxyConnectExecutor.create(
                sessions,
                getClientBuilder().getConnectionClientExecutors());
        return new SimpleServerExecutor<ProxySessionExecutor>(
                sessions,
                connectExecutor,
                getDefaultAnonymousExecutor());
    }

    protected TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> getDefaultAnonymousExecutor() {
        return ProcessorTaskExecutor.of(FourLetterRequestProcessor.newInstance());
    }
}

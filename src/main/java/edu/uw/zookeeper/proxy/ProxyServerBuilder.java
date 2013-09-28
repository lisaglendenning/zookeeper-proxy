package edu.uw.zookeeper.proxy;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.MapMaker;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.*;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.protocol.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.server.ServerConnectionFactoryBuilder;

public class ProxyServerBuilder extends ServerConnectionExecutorsService.Builder {

    public static ProxyServerBuilder defaults() {
        return new ProxyServerBuilder();
    }
    
    @Configurable(arg="servers", key="servers", value="127.0.0.1:2081", help="address:port,...")
    public static class ConfigurableEnsembleView extends edu.uw.zookeeper.client.ConfigurableEnsembleView {

        public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
            return new ConfigurableEnsembleView().apply(configuration);
        }
    }

    public static class ServerViewFactories extends ForwardingService implements ParameterizedFactory<ServerInetAddressView, ServerViewFactory<ConnectMessage.Request, ?>> {

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
        public ServerViewFactory<ConnectMessage.Request, ?> get(ServerInetAddressView view) {
            return ServerViewFactory.newInstance(
                    view, 
                    ServerViewFactory.FromRequestFactory.create(
                            FixedClientConnectionFactory.create(view.get(), connections),
                            executor), 
                    ZxidTracker.create());
        }

        @Override
        protected ClientConnectionFactory<? extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> delegate() {
            return connections;
        }
    }

    protected final Logger logger = LogManager.getLogger(getClass());
    protected final NettyModule netModule;
    protected final ClientConnectionFactoryBuilder clientConnectionBuilder;
    
    protected ProxyServerBuilder() {
        this(null, null, null, null, null, null, null);
    }

    protected ProxyServerBuilder(
            NettyModule netModule,
            ClientConnectionFactoryBuilder clientConnectionBuilder,
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor,
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
            RuntimeModule runtime) {
        super(connectionBuilder, serverConnectionFactory, serverTaskExecutor,
                connectionExecutors, runtime);
        this.netModule = netModule;
        this.clientConnectionBuilder = clientConnectionBuilder;
    }

    @Override
    public ProxyServerBuilder setRuntimeModule(RuntimeModule runtime) {
        return (ProxyServerBuilder) super.setRuntimeModule(runtime);
    }
    
    public ProxyServerBuilder setNetModule(NettyModule netModule) {
        return newInstance(netModule, clientConnectionBuilder, connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
    }

    public ProxyServerBuilder setClientConnectionFactoryBuilder(
            ClientConnectionFactoryBuilder clientConnectionBuilder) {
        return newInstance(netModule, clientConnectionBuilder, connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
    }

    @Override
    public ProxyServerBuilder setDefaults() {
        if (netModule == null) {
            return setNetModule(getDefaultNetModule()).setDefaults();
        }
        if (clientConnectionBuilder == null) {
            return setClientConnectionFactoryBuilder(getDefaultClientConnectionFactoryBuilder()).setDefaults();
        }
        return (ProxyServerBuilder) super.setDefaults();
    }
    
    @Override
    protected ProxyServerBuilder newInstance(
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor,
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
            RuntimeModule runtime) {
        return newInstance(netModule, clientConnectionBuilder, connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
    }
    
    protected ProxyServerBuilder newInstance(
            NettyModule netModule,
            ClientConnectionFactoryBuilder clientConnectionBuilder,
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor,
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
            RuntimeModule runtime) {
        return new ProxyServerBuilder(netModule, clientConnectionBuilder, connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
    }

    protected NettyModule getDefaultNetModule() {
        return NettyModule.newInstance(getRuntimeModule());
    }

    protected ClientConnectionFactoryBuilder getDefaultClientConnectionFactoryBuilder() {
        return ClientConnectionFactoryBuilder.defaults()
                .setClientModule(netModule.clients())
                .setConnectionFactory(ProtocolCodecConnection.<Operation.Request,AssignXidCodec,Connection<Operation.Request>>factory())
                .setRuntimeModule(runtime)
                .setDefaults();
    }

    @Override
    protected ServerTaskExecutor getDefaultServerTaskExecutor() {
        EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(getRuntimeModule().getConfiguration());
        ServerViewFactories serverFactory = 
                ServerViewFactories.newInstance(
                        clientConnectionBuilder.build(),
                        getRuntimeModule().getExecutors().get(ScheduledExecutorService.class));
        getRuntimeModule().getServiceMonitor().add(serverFactory);
        EnsembleViewFactory<ServerViewFactory<ConnectMessage.Request, ? extends ClientConnectionExecutor<?>>> ensembleFactory = 
                EnsembleViewFactory.newInstance(
                        ensemble,
                        EnsembleViewFactory.RandomSelector.<ServerInetAddressView>newInstance(),
                        serverFactory);
        ConcurrentMap<Long, Publisher> listeners = new MapMaker().makeMap();
        ConcurrentMap<Long, ClientConnectionExecutor<?>> clients = new MapMaker().makeMap();
        TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                ServerTaskExecutor.ProcessorExecutor.of(FourLetterRequestProcessor.newInstance());
        ProxyConnectExecutor connectExecutor = ProxyConnectExecutor.newInstance(
                getRuntimeModule().getExecutors().get(ExecutorService.class), 
                listeners, 
                clients, 
                ensembleFactory);
        ProxyRequestExecutor sessionExecutor = ProxyRequestExecutor.newInstance(clients);
        return ServerTaskExecutor.newInstance(anonymousExecutor, connectExecutor, sessionExecutor);
    }
    
    @Override
    protected ServerConnectionFactoryBuilder getDefaultServerConnectionFactoryBuilder() {
        return ServerConnectionFactoryBuilder.defaults()
                .setTimeOut(clientConnectionBuilder.getTimeOut())
                .setServerModule(netModule.servers())
                .setRuntimeModule(runtime)
                .setDefaults();
    }
}

package edu.uw.zookeeper.proxy;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import com.google.common.collect.MapMaker;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Operation.Request;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.PingingClient;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.protocol.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.proxy.netty.NettyModule;
import edu.uw.zookeeper.server.ServerApplicationModule;

public enum ProxyApplicationModule implements ParameterizedFactory<RuntimeModule, Application> {
    INSTANCE;
    
    public static ProxyApplicationModule getInstance() {
        return INSTANCE;
    }

    public static class ServerViewFactories<V extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> implements ParameterizedFactory<V, ServerViewFactory<ConnectMessage.Request, V, C>> {

        public static <V extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> ServerViewFactories<V,C> newInstance(
                ClientConnectionFactory<C> connections) {
            return new ServerViewFactories<V,C>(connections);
        }
        
        protected final ClientConnectionFactory<C> connections;
        
        protected ServerViewFactories(
                ClientConnectionFactory<C> connections) {
            this.connections = connections;
        }

        @Override
        public ServerViewFactory<ConnectMessage.Request, V, C> get(V view) {
            return ServerViewFactory.newInstance(
                    view, 
                    ServerViewFactory.FromRequestFactory.create(
                            FixedClientConnectionFactory.create(view.get(), connections)), 
                    ZxidTracker.create());
        }
    }

    public static <C extends Connection<? super Message.ClientSession>> ServerTaskExecutor getServerExecutor(
            Executor executor,
            EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<ConnectMessage.Request, ServerInetAddressView, C>> clientFactory) {
        ConcurrentMap<Long, Publisher> listeners = new MapMaker().makeMap();
        ConcurrentMap<Long, ClientConnectionExecutor<C>> clients = new MapMaker().makeMap();
        TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                ServerTaskExecutor.ProcessorExecutor.of(FourLetterRequestProcessor.getInstance());
        ProxyConnectExecutor<ServerInetAddressView, C> connectExecutor = ProxyConnectExecutor.newInstance(executor, listeners, clients, clientFactory);
        ProxyRequestExecutor<C> sessionExecutor = ProxyRequestExecutor.newInstance(clients);
        return ServerTaskExecutor.newInstance(anonymousExecutor, connectExecutor, sessionExecutor);
    }
    
    @Override
    public Application get(RuntimeModule runtime) {
        ServiceMonitor monitor = runtime.serviceMonitor();
        AbstractMain.MonitorServiceFactory monitorsFactory = AbstractMain.monitors(monitor);

        NettyModule netModule = NettyModule.newInstance(runtime);
        
        // Client
        TimeValue timeOut = ClientApplicationModule.TimeoutFactory.newInstance().get(runtime.configuration());
        ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory = ClientApplicationModule.codecFactory();
        ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnectionFactory =
                new ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>() {
                    @Override
                    public ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Request>> get(
                            Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>> value) {
                        return ProtocolCodecConnection.newInstance(value.first().second(), value.second());
                    }
        };
                PingingClient.factory(timeOut, runtime.executors().asScheduledExecutorServiceFactory().get());
        ClientConnectionFactory<ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                monitorsFactory.apply(
                    netModule.clients().getClientConnectionFactory(
                            codecFactory, clientConnectionFactory).get());
        ServerViewFactories<ServerInetAddressView, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> serverFactory = 
                ServerViewFactories.newInstance(clientConnections);
        EnsembleView<ServerInetAddressView> ensemble = ClientApplicationModule.ConfigurableEnsembleViewFactory.newInstance().get(runtime.configuration());
        EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<ConnectMessage.Request, ServerInetAddressView, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>> ensembleFactory = 
                EnsembleViewFactory.newInstance(
                        ensemble,
                        EnsembleViewFactory.RandomSelector.<ServerInetAddressView>newInstance(),
                        ServerInetAddressView.class,
                        serverFactory);

        // Proxy 
        ServerTaskExecutor serverExecutor = getServerExecutor(
                runtime.executors().asListeningExecutorServiceFactory().get(),
                ensembleFactory);
        
        // Server
        ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>>> serverConnectionFactory = 
                netModule.servers().getServerConnectionFactory(
                        ServerApplicationModule.codecFactory(),
                        ServerApplicationModule.connectionFactory());
        ServerInetAddressView address = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(runtime.configuration());
        ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections = 
                monitorsFactory.apply(serverConnectionFactory.get(address.get()));
        monitorsFactory.apply(ServerConnectionExecutorsService.newInstance(serverConnections, serverExecutor));

        return ServiceApplication.newInstance(runtime.serviceMonitor());
    }
}

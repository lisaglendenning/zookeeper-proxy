package edu.uw.zookeeper.proxy;

import java.net.SocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;

import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.client.ClientApplicationModule.ConfigurableEnsembleView;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceApplication;
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
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.Operation.Request;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.protocol.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.proxy.netty.NettyModule;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.server.ServerApplicationModule.ConfigurableServerAddressView;

public class ProxyApplicationModule implements Callable<Application> {

    public static ParameterizedFactory<RuntimeModule, Application> factory() {
        return new ParameterizedFactory<RuntimeModule, Application>() {
            @Override
            public Application get(RuntimeModule runtime) {
                try {
                    return new ProxyApplicationModule(runtime).call();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }  
        };
    }
    
    public static class ServerViewFactories<V extends ServerView.Address<? extends SocketAddress>, C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> implements ParameterizedFactory<V, ServerViewFactory<ConnectMessage.Request, V, C>> {

        public static <V extends ServerView.Address<? extends SocketAddress>, C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ServerViewFactories<V,C> newInstance(
                ClientConnectionFactory<C> connections,
                ScheduledExecutorService executor) {
            return new ServerViewFactories<V,C>(connections, executor);
        }
        
        protected final ClientConnectionFactory<C> connections;
        protected final ScheduledExecutorService executor;
        
        protected ServerViewFactories(
                ClientConnectionFactory<C> connections,
                ScheduledExecutorService executor) {
            this.connections = connections;
            this.executor = executor;
        }

        @Override
        public ServerViewFactory<ConnectMessage.Request, V, C> get(V view) {
            return ServerViewFactory.newInstance(
                    view, 
                    ServerViewFactory.FromRequestFactory.create(
                            FixedClientConnectionFactory.create(view.get(), connections),
                            executor), 
                    ZxidTracker.create());
        }
    }

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ServerTaskExecutor getServerExecutor(
            Executor executor,
            EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<ConnectMessage.Request, ServerInetAddressView, C>> clientFactory) {
        ConcurrentMap<Long, Publisher> listeners = new MapMaker().makeMap();
        ConcurrentMap<Long, ClientConnectionExecutor<C>> clients = new MapMaker().makeMap();
        TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                ServerTaskExecutor.ProcessorExecutor.of(FourLetterRequestProcessor.newInstance());
        ProxyConnectExecutor<ServerInetAddressView, C> connectExecutor = ProxyConnectExecutor.newInstance(executor, listeners, clients, clientFactory);
        ProxyRequestExecutor<C> sessionExecutor = ProxyRequestExecutor.newInstance(clients);
        return ServerTaskExecutor.newInstance(anonymousExecutor, connectExecutor, sessionExecutor);
    }

    protected final RuntimeModule runtime;
    
    public ProxyApplicationModule(RuntimeModule runtime) {
        this.runtime = runtime;
    }
    
    public RuntimeModule getRuntime() {
        return runtime;
    }

    protected TimeValue getTimeOut() {
        TimeValue value = DefaultMain.ConfigurableTimeout.get(runtime.configuration());
        return value;
    }
    
    @Override
    public Application call() throws Exception {
        NettyModule netModule = NettyModule.newInstance(runtime);
        
        // Client
        TimeValue timeOut = getTimeOut();
        ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory = AssignXidCodec.factory();
        ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnectionFactory =
                new ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>() {
                    @Override
                    public ProtocolCodecConnection<Operation.Request, AssignXidCodec, Connection<Request>> get(
                            Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>> value) {
                        return ProtocolCodecConnection.newInstance(value.first().second(), value.second());
                    }
        };
        ClientConnectionFactory<ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                netModule.clients().getClientConnectionFactory(
                            codecFactory, clientConnectionFactory).get();
        runtime.serviceMonitor().add(clientConnections);
        ServerViewFactories<ServerInetAddressView, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> serverFactory = 
                ServerViewFactories.newInstance(clientConnections, runtime.executors().asScheduledExecutorServiceFactory().get());
        EnsembleView<ServerInetAddressView> ensemble = ClientApplicationModule.ConfigurableEnsembleView.get(runtime.configuration());
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
        ServerInetAddressView address = ServerApplicationModule.ConfigurableServerAddressView.get(runtime.configuration());
        ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections = 
                serverConnectionFactory.get(address.get());
        runtime.serviceMonitor().add(serverConnections);
        runtime.serviceMonitor().add(ServerConnectionExecutorsService.newInstance(
                serverConnections, 
                timeOut,
                runtime.executors().asScheduledExecutorServiceFactory().get(),
                serverExecutor));

        return ServiceApplication.newInstance(runtime.serviceMonitor());
    }
}

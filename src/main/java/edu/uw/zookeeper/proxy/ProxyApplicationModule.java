package edu.uw.zookeeper.proxy;

import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.clients.common.RuntimeModuleProvider;
import edu.uw.zookeeper.clients.trace.JacksonModule;
import edu.uw.zookeeper.clients.trace.ProtocolTracingCodec;
import edu.uw.zookeeper.clients.trace.Trace;
import edu.uw.zookeeper.clients.trace.TraceEvent;
import edu.uw.zookeeper.clients.trace.TraceEventPublisherService;
import edu.uw.zookeeper.clients.trace.TraceEventTag;
import edu.uw.zookeeper.clients.trace.TraceHeader;
import edu.uw.zookeeper.clients.trace.TraceWriter;
import edu.uw.zookeeper.clients.trace.TraceClientModule.TraceDescriptionConfiguration;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
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
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.proxy.netty.NettyModule;
import edu.uw.zookeeper.server.ServerApplicationModule;

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

    @Configurable(arg="ensemble", key="Ensemble", value="localhost:2081", help="Address:Port,...")
    public static class ConfigurableEnsembleView extends ClientApplicationModule.ConfigurableEnsembleView {

        public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
            return new ConfigurableEnsembleView().apply(configuration);
        }
    }
    
    @Configurable(arg="trace", key="DoTrace", value="true", type=ConfigValueType.BOOLEAN)
    public static class DoTraceConfiguration implements Function<Configuration, Boolean> {

        public static Boolean get(Configuration configuration) {
            return new DoTraceConfiguration().apply(configuration);
        }

        @Override
        public Boolean apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getBoolean(configurable.key());
        }
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

    public class Module extends AbstractModule {

        @Override
        protected void configure() {
            install(JacksonModule.create());
            bind(new TypeLiteral<Actor<TraceEvent>>() {}).to(TraceWriter.class);
        }

        @Provides @Singleton
        public TraceHeader getTraceHeader(Configuration configuration) {
            return TraceHeader.create(
                    TraceDescriptionConfiguration.get(configuration), 
                    TraceEventTag.TIMESTAMP_EVENT, 
                    TraceEventTag.PROTOCOL_REQUEST_EVENT, 
                    TraceEventTag.PROTOCOL_RESPONSE_EVENT);
        }
        
        @Provides @Singleton
        public TraceWriter getTraceWriter(
                Configuration configuration,
                ObjectMapper mapper,
                TraceHeader header,
                Executor executor) throws IOException {
            File file = Trace.getTraceOutputFileConfiguration(configuration);
            logger.info("Trace output: {}", file);
            return TraceWriter.forFile(
                    file, 
                    mapper.writer(), 
                    header,
                    executor);
        }

        @Provides @Singleton
        public Publisher getTracePublisher(
                Factory<? extends Publisher> publishers) {
            return publishers.get();
        }
        
        @Provides @Singleton
        public TraceEventPublisherService getTraceEventWriterService(
                Actor<TraceEvent> writer, Publisher publisher) {
            return TraceEventPublisherService.newInstance(publisher, writer);
        }
        
        @Provides @Singleton
        public TimeValue getTimeOut(Configuration configuration) {
            return DefaultMain.ConfigurableTimeout.get(configuration);
        }
        
        @Provides @Singleton
        public NettyModule getNetModule(RuntimeModule runtime) {
            return NettyModule.newInstance(runtime);
        }

        @Provides @Singleton
        public NetClientModule getNetClientModule(NettyModule net) {
            return net.clients();
        }

        @Provides @Singleton
        public NetServerModule getNetServerModule(NettyModule net) {
            return net.servers();
        }
        
        @Provides @Singleton
        public ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getCodecFactory(
                Injector injector,
                Configuration configuration) {
            if (DoTraceConfiguration.get(configuration)) {
                injector.getInstance(ServiceMonitor.class).add(injector.getInstance(TraceEventPublisherService.class));
                return ProtocolTracingCodec.factory(injector.getInstance(Publisher.class));
            } else {
                return AssignXidCodec.factory();
            }
        }
        
        @Provides @Singleton
        public ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getProtocolConnectionFactory() {
            return ProtocolCodecConnection.factory();
        }
        
        @Provides @Singleton
        public ClientConnectionFactory<ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getClientConnectionFactory(
                ServiceMonitor monitor,
                NetClientModule clientModule,
                ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory,
                ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connectionFactory) {
            ClientConnectionFactory<ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connections = clientModule.getClientConnectionFactory(
                                codecFactory,
                                connectionFactory).get();
            monitor.add(connections);
            return connections;
        }
        
        @Provides @Singleton
        public EnsembleView<ServerInetAddressView> getEnsembleView(
                Configuration configuration) {
            return ConfigurableEnsembleView.get(configuration);
        }
        
        @Provides @Singleton
        public ServerTaskExecutor getServerTaskExecutor(
                Executor executor,
                ScheduledExecutorService scheduled,
                EnsembleView<ServerInetAddressView> ensemble,
                ClientConnectionFactory<ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connections) {
            ServerViewFactories<ServerInetAddressView, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> serverFactory = 
                    ServerViewFactories.newInstance(connections, scheduled);
            EnsembleViewFactory<ServerInetAddressView, ServerViewFactory<ConnectMessage.Request, ServerInetAddressView, ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>> ensembleFactory = 
                    EnsembleViewFactory.newInstance(
                            ensemble,
                            EnsembleViewFactory.RandomSelector.<ServerInetAddressView>newInstance(),
                            ServerInetAddressView.class,
                            serverFactory);
            return getServerExecutor(
                    executor,
                    ensembleFactory);
        }
        
        @Provides @Singleton
        public ServerInetAddressView getServerAddress(Configuration configuration) {
            return ServerApplicationModule.ConfigurableServerAddressView.get(configuration);
        }
        
        @Provides @Singleton
        public ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> getServerConnectionFactory(
                NetServerModule serverModule,
                ServerInetAddressView address,
                ServiceMonitor monitor) {
            ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>>> serverConnectionFactory = 
                    serverModule.getServerConnectionFactory(
                            ServerApplicationModule.codecFactory(),
                            ServerApplicationModule.connectionFactory());
            ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections = 
                    serverConnectionFactory.get(address.get());
            return connections;
        }
        
        @Provides @Singleton
        public ServerConnectionExecutorsService<?> getServerConnectionExecutorsService(
                ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections,
                TimeValue timeOut,
                ServerTaskExecutor server,
                ScheduledExecutorService scheduled,
                ServiceMonitor monitor) {
            ServerConnectionExecutorsService<?> instance = ServerConnectionExecutorsService.newInstance(
                    connections, 
                    timeOut,
                    scheduled,
                    server);
            monitor.add(instance);
            return instance;
        }
    }
    
    protected final RuntimeModule runtime;
    protected final Logger logger = LogManager.getLogger(getClass());
    protected final Injector injector;
    
    public ProxyApplicationModule(RuntimeModule runtime) {
        this.runtime = runtime;
        this.injector = createInjector(runtime);
    }
    
    public RuntimeModule getRuntime() {
        return runtime;
    }

    public Injector getInjector() {
        return injector;
    }

    protected Injector createInjector(RuntimeModule runtime) {
        return Guice.createInjector(
                RuntimeModuleProvider.create(runtime), 
                module());
    }
    
    protected com.google.inject.Module module() {
        return new Module();
    }

    @Override
    public Application call() throws Exception {
        injector.getInstance(Key.get(new TypeLiteral<ServerConnectionExecutorsService<?>>(){}));
        return ServiceApplication.newInstance(runtime.serviceMonitor());
    }
}

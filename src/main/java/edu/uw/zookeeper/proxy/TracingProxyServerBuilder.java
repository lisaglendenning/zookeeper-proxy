package edu.uw.zookeeper.proxy;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.clients.trace.ProtocolTracingCodec;
import edu.uw.zookeeper.clients.trace.TraceWriterBuilder;
import edu.uw.zookeeper.clients.trace.Tracing;
import edu.uw.zookeeper.clients.trace.TraceEventPublisherService;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message.Server;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.proxy.netty.NettyModule;
import edu.uw.zookeeper.server.ServerConnectionFactoryBuilder;

public class TracingProxyServerBuilder extends ProxyServerBuilder {

    public static ProxyServerBuilder fromRuntimeModule(RuntimeModule runtime) {
        boolean doTrace = DoTraceConfiguration.get(runtime.getConfiguration());
        ProxyServerBuilder builder = doTrace ? TracingProxyServerBuilder.defaults() : ProxyServerBuilder.defaults();
        return builder.setRuntimeModule(runtime);
    }
    
    public static TracingProxyServerBuilder defaults() {
        return new TracingProxyServerBuilder();
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

    protected final TracingBuilder tracingBuilder;
    
    protected TracingProxyServerBuilder() {
        this(null, null, null, null, null, null, null, null);
    }

    protected TracingProxyServerBuilder(
            TracingBuilder tracingBuilder,
            NettyModule netModule,
            ClientConnectionFactoryBuilder clientConnectionBuilder,
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Server, ServerProtocolCodec, Connection<Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor,
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Server, ServerProtocolCodec, Connection<Server>>> connectionExecutors,
            RuntimeModule runtime) {
        super(netModule, clientConnectionBuilder, connectionBuilder,
                serverConnectionFactory, serverTaskExecutor, connectionExecutors,
                runtime);
        this.tracingBuilder = tracingBuilder;
    }
    
    public TracingBuilder getTracingBuilder() {
        return tracingBuilder;
    }

    public TracingProxyServerBuilder setTracingBuilder(
            TracingBuilder tracingBuilder) {
        return newInstance(tracingBuilder, netModule, clientConnectionBuilder, connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
    }

    @Override
    public TracingProxyServerBuilder setDefaults() {
        if (tracingBuilder == null) {
            return setTracingBuilder(getDefaultTracingBuilder()).setDefaults();
        }
        return (TracingProxyServerBuilder) super.setDefaults();
    }

    @Override
    protected TracingProxyServerBuilder newInstance(
            NettyModule netModule,
            ClientConnectionFactoryBuilder clientConnectionBuilder,
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor,
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
            RuntimeModule runtime) {
        return newInstance(tracingBuilder, netModule, clientConnectionBuilder, connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
    }
    
    protected TracingProxyServerBuilder newInstance(
            TracingBuilder tracingBuilder,
            NettyModule netModule,
            ClientConnectionFactoryBuilder clientConnectionBuilder,
            ServerConnectionFactoryBuilder connectionBuilder,
            ServerConnectionFactory<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnectionFactory,
            ServerTaskExecutor serverTaskExecutor,
            ServerConnectionExecutorsService<? extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connectionExecutors,
            RuntimeModule runtime) {
        return new TracingProxyServerBuilder(tracingBuilder, netModule, clientConnectionBuilder, connectionBuilder, serverConnectionFactory, serverTaskExecutor, connectionExecutors, runtime);
    }
    
    @Override
    protected List<Service> getServices() {
        List<Service> services = super.getServices();
        services.add(0, tracingBuilder.build());
        return services;
    }
    
    protected TracingBuilder getDefaultTracingBuilder() {
        return TracingBuilder.defaults().setRuntimeModule(getRuntimeModule());
    }
    
    @Override
    protected ClientConnectionFactoryBuilder getDefaultClientConnectionFactoryBuilder() {
        return ClientConnectionFactoryBuilder.defaults()
                .setClientModule(netModule.clients())
                .setCodecFactory(ProtocolTracingCodec.factory(tracingBuilder.getTracePublisher().getPublisher()))
                .setConnectionFactory(ProtocolCodecConnection.<Operation.Request,AssignXidCodec,Connection<Operation.Request>>factory())
                .setRuntimeModule(runtime)
                .setDefaults();
    }
    
    public static class TracingBuilder extends Tracing.TraceWritingBuilder<TraceEventPublisherService, TracingBuilder> {

        public static TracingBuilder defaults() {
            return new TracingBuilder(null, null, null, null);
        }
        
        protected TracingBuilder(
                TraceWriterBuilder writerBuilder,
                TraceEventPublisherService tracePublisher, 
                ObjectMapper mapper,
                RuntimeModule runtime) {
            super(writerBuilder, tracePublisher, mapper, runtime);
        }

        @Override
        protected TracingBuilder newInstance(TraceWriterBuilder writerBuilder,
                TraceEventPublisherService tracePublisher, ObjectMapper mapper,
                RuntimeModule runtime) {
            return new TracingBuilder(writerBuilder, tracePublisher, mapper, runtime);
        }

        @Override
        protected TraceEventPublisherService doBuild() {
            return tracePublisher;
        }
    }
}

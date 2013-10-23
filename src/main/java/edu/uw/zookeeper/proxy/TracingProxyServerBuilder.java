package edu.uw.zookeeper.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
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
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;

public class TracingProxyServerBuilder extends ProxyServerExecutorBuilder {

    public static ProxyServerExecutorBuilder fromRuntimeModule(RuntimeModule runtime) {
        boolean doTrace = DoTraceConfiguration.get(runtime.getConfiguration());
        ProxyServerExecutorBuilder builder = doTrace ? TracingProxyServerBuilder.defaults() : ProxyServerExecutorBuilder.defaults();
        return builder.setRuntimeModule(runtime);
    }
    
    public static TracingProxyServerBuilder defaults() {
        return new TracingProxyServerBuilder(null, null, ClientBuilder.defaults());
    }
    
    @Configurable(arg="trace", key="doTrace", value="true", type=ConfigValueType.BOOLEAN)
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
    
    protected TracingProxyServerBuilder(
            TracingBuilder tracingBuilder,
            NettyModule netModule,
            ClientBuilder clientBuilder) {
        super(netModule, clientBuilder);
        this.tracingBuilder = tracingBuilder;
    }
    
    public TracingBuilder getTracingBuilder() {
        return tracingBuilder;
    }

    public TracingProxyServerBuilder setTracingBuilder(
            TracingBuilder tracingBuilder) {
        return newInstance(tracingBuilder, netModule, delegate);
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
            ClientBuilder clientBuilder) {
        return newInstance(tracingBuilder, netModule, clientBuilder);
    }
    
    protected TracingProxyServerBuilder newInstance(
            TracingBuilder tracingBuilder,
            NettyModule netModule,
            ClientBuilder clientBuilder) {
        return new TracingProxyServerBuilder(tracingBuilder, netModule, clientBuilder);
    }
    
    protected TracingBuilder getDefaultTracingBuilder() {
        return TracingBuilder.defaults().setRuntimeModule(getRuntimeModule()).setDefaults();
    }

    @Override
    protected ClientBuilder getDefaultClientBuilder() {
        ClientBuilder builder = getClientBuilder();
        if (builder.getConnectionBuilder() == null) {
            builder = builder.setConnectionBuilder(
                    ClientConnectionFactoryBuilder.defaults()
                        .setClientModule(netModule.clients())
                        .setCodecFactory(ProtocolTracingCodec.factory(getTracingBuilder().getTracePublisher().getPublisher()))
                        .setConnectionFactory(ClientProtocolConnection.<Message.ClientSession, ProtocolCodec<Message.ClientSession, Message.ServerSession>, Connection<Message.ClientSession>>factory()));
        }
        return builder.setDefaults();
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
            return getTracePublisher();
        }
    }
}

package edu.uw.zookeeper.proxy;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;
import edu.uw.zookeeper.server.SimpleServerBuilder;

public class ProxyServerBuilder extends SimpleServerBuilder<ProxyServerExecutorBuilder> {

    public static ProxyServerBuilder defaults() {
        return new ProxyServerBuilder(
                ProxyServerExecutorBuilder.defaults(),
                ServerConnectionsHandler.Builder.defaults());
    }
    
    protected ProxyServerBuilder(
            ProxyServerExecutorBuilder server,
            ServerConnectionsHandler.Builder connections) {
        super(server, connections);
    }

    @Override
    public ProxyServerBuilder setRuntimeModule(RuntimeModule runtime) {
        return newInstance(
                TracingProxyServerBuilder.fromRuntimeModule(runtime), 
                getConnectionsBuilder().setRuntimeModule(runtime));
    }

    @Override
    protected ProxyServerBuilder newInstance(
            ProxyServerExecutorBuilder server,
            ServerConnectionsHandler.Builder connections) {
        return new ProxyServerBuilder(server, connections);
    }

    @Override
    protected List<Service> doBuild() {
        List<Service> services = Lists.newLinkedList();
        if (getServerBuilder() instanceof TracingProxyServerBuilder) {
            services.add(((TracingProxyServerBuilder) getServerBuilder()).getTracingBuilder().build());
        }
        for (Service e: getServerBuilder().getClientBuilder().build()) {
            services.add(e);
        }
        for (Service e: super.doBuild()) {
            services.add(e);
        }
        return services;
    }
    
    @Override
    protected ServerConnectionsHandler.Builder getDefaultConnectionsBuilder() {
        ServerConnectionsHandler.Builder connections = getConnectionsBuilder();
        if (connections.getServerExecutor() == null) {
            connections = connections.setServerExecutor(getDefaultServerExecutor());
        }
        return connections
                .setConnectionBuilder(connections.getConnectionBuilder().setServerModule(getServerBuilder().getNetModule().servers()))
                .setTimeOut(getServerBuilder().getClientBuilder().getConnectionBuilder().getTimeOut())
                .setDefaults();
    }
}

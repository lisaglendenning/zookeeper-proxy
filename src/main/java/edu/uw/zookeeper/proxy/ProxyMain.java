package edu.uw.zookeeper.proxy;


import java.net.SocketAddress;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.client.EnsembleFactory;
import edu.uw.zookeeper.client.ClientMain;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.ExpireSessionsTask;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.Server;
import edu.uw.zookeeper.server.SessionParametersPolicy;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.Singleton;
import edu.uw.zookeeper.util.TimeValue;

public abstract class ProxyMain extends AbstractMain {

    protected final Singleton<Application> application;
    
    protected ProxyMain(Configuration configuration) {
        super(configuration);
        this.application = Factories.lazyFrom(new Factory<Application>() {
            @Override
            public Application get() {
                ServiceMonitor monitor = serviceMonitor();
                MonitorServiceFactory monitorsFactory = monitors(monitor);

                // Client
                ClientConnectionFactory clientConnections = monitorsFactory.apply(clientConnectionFactory().get());
                TimeValue timeOut = ClientMain.TimeoutFactory.newInstance().get(configuration());
                EnsembleView ensemble = ConfigurableEnsembleViewFactory.newInstance().get(configuration());
                ParameterizedFactory<Connection, PingingClientCodecConnection> codecFactory = PingingClientCodecConnection.factory(
                        publisherFactory(), timeOut, executors().asScheduledExecutorServiceFactory().get());
                EnsembleFactory ensembleFactory = EnsembleFactory.newInstance(clientConnections, codecFactory, ensemble, timeOut);
                Factory<ClientProtocolConnection> clientFactory = ensembleFactory.get();

                // Server
                ServerView.Address<?> address = ConfigurableServerAddressViewFactory.newInstance().get(configuration());
                ServerConnectionFactory serverConnections = monitorsFactory.apply(serverConnectionFactory().get(address.get()));
                SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(configuration());
                ExpiringSessionManager sessions = ExpiringSessionManager.newInstance(publisherFactory.get(), policy);
                ExpireSessionsTask expires = monitorsFactory.apply(ExpireSessionsTask.newInstance(sessions, executors.asScheduledExecutorServiceFactory().get(), configuration()));
                final ProxyServerExecutor serverExecutor = ProxyServerExecutor.newInstance(
                        executors.asListeningExecutorServiceFactory().get(), publisherFactory(), sessions, clientFactory);
                final Server server = Server.newInstance(publisherFactory(), serverConnections, serverExecutor);
                
                return ProxyMain.super.application();
            }
        });
    }

    @Override
    protected Application application() {
        return application.get();
    }
    
    protected abstract Factory<? extends ClientConnectionFactory> clientConnectionFactory();

    protected abstract ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory> serverConnectionFactory();
}
package edu.uw.zookeeper.proxy;


import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.Connection;
import edu.uw.zookeeper.RequestExecutorService;
import edu.uw.zookeeper.ServiceMain;
import edu.uw.zookeeper.Xid;
import edu.uw.zookeeper.Zxid;
import edu.uw.zookeeper.client.ClientConnectionFactory;
import edu.uw.zookeeper.client.ClientSessionConnection;
import edu.uw.zookeeper.protocol.client.PingSessionsTask;
import edu.uw.zookeeper.server.ConnectionManager;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.ExpireSessionsTask;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.SessionManager;
import edu.uw.zookeeper.server.SessionParametersPolicy;
import edu.uw.zookeeper.util.ServiceMonitor;

public class ProxyMain extends ServiceMain {

    public static void main(String[] args) throws Exception {
        ProxyMain main = get();
        main.apply(args);
    }

    public static ProxyMain get() {
        return new ProxyMain();
    }

    protected ProxyMain() {
    }

    @Override
    protected void configure() {
        super.configure();

        // server
        bind(Zxid.class).in(Singleton.class);
        bind(ExpiringSessionManager.class).in(Singleton.class);
        bind(ExpireSessionsTask.class).in(Singleton.class);
        bind(SessionParametersPolicy.class).to(
                DefaultSessionParametersPolicy.class);
        bind(RequestExecutorService.Factory.class).to(
                ProxyRequestExecutor.Factory.class).in(Singleton.class);
        bind(ConnectionManager.class).asEagerSingleton();
        // bind(ExpireSessionsTask.class).asEagerSingleton();

        // client
        bind(Xid.class).in(Singleton.class);
        bind(ClientConnectionFactory.class).asEagerSingleton();
        bind(ClientSessionConnection.ConnectionFactory.class).in(
                Singleton.class);
        bind(Connection.class).toProvider(ClientConnectionFactory.class);
        bind(ClientSessionConnection.class).toProvider(
                ClientSessionConnection.ConnectionFactory.class);
        bind(PingSessionsTask.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public SessionManager getSessionManager(ExpiringSessionManager manager,
            ExpireSessionsTask task, ServiceMonitor monitor) {
        monitor.add(task);
        return manager;
    }

    @Provides
    @Singleton
    public Service getService(ServiceMonitor monitor) {
        return monitor;
    }
}

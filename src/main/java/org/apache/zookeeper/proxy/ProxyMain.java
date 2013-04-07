package org.apache.zookeeper.proxy;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.RequestExecutorService;
import org.apache.zookeeper.ServiceMain;
import org.apache.zookeeper.Xid;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.client.ClientConnectionFactory;
import org.apache.zookeeper.client.ClientSessionConnection;
import org.apache.zookeeper.client.SingleClientConnectionFactory;
import org.apache.zookeeper.protocol.client.PingSessionsTask;
import org.apache.zookeeper.server.ConnectionManager;
import org.apache.zookeeper.server.DefaultSessionParametersPolicy;
import org.apache.zookeeper.server.ExpireSessionsTask;
import org.apache.zookeeper.server.ExpiringSessionManager;
import org.apache.zookeeper.server.SessionManager;
import org.apache.zookeeper.server.SessionParametersPolicy;
import org.apache.zookeeper.util.ServiceMonitor;

import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;

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

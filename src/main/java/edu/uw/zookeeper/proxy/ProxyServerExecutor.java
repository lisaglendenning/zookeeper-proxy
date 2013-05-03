package edu.uw.zookeeper.proxy;

import java.io.IOException;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListeningExecutorService;

import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.client.SessionClient;
import edu.uw.zookeeper.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.ServerExecutor;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Publisher;

public class ProxyServerExecutor extends ServerExecutor {

    public static ProxyServerExecutor newInstance(
            final ListeningExecutorService executor,
            final Factory<Publisher> publisherFactory,
            final ExpiringSessionManager sessions,
            final Factory<SessionClient> clientFactory) {
        AssignZxidProcessor zxids = AssignZxidProcessor.newInstance();
        AssignXidProcessor xids = AssignXidProcessor.newInstance();
        return newInstance(
                executor,
                publisherFactory,
                sessions, 
                zxids,
                xids,
                clientFactory);
    }
    
    public static ProxyServerExecutor newInstance(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids,
            AssignXidProcessor xids,
            Factory<SessionClient> clientFactory) {
        return new ProxyServerExecutor(
                executor,
                publisherFactory,
                sessions,
                zxids, 
                xids, 
                clientFactory);
    }
    
    protected final Factory<SessionClient> clientFactory;
    protected final AssignXidProcessor xids;
    
    protected ProxyServerExecutor(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids,
            AssignXidProcessor xids,
            Factory<SessionClient> clientFactory) {
        super(executor, publisherFactory, sessions, zxids);
        this.xids = xids;
        this.clientFactory = clientFactory;
    }
    
    public AssignXidProcessor xids() {
        return xids;
    }
    
    @Override
    protected PublishingSessionRequestExecutor newSessionRequestExecutor(Long sessionId) {
        SessionClient client = clientFactory.get();
        try {
            return ProxyRequestExecutor.newInstance(
                    publisherFactory.get(), this, sessionId, client);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}

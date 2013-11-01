package edu.uw.zookeeper.proxy;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.MessageClientExecutor;
import edu.uw.zookeeper.protocol.server.SessionExecutor;

public class ProxySessionExecutor implements SessionExecutor {

    public static ProxySessionExecutor newInstance(MessageClientExecutor<?> client) {
        return new ProxySessionExecutor(client);
    }

    protected final MessageClientExecutor<?> client;
    
    public ProxySessionExecutor(
            MessageClientExecutor<?> client) {
        this.client = client;
    }

    @Override
    public Session session() {
        try {
            return client.session().get().toSession();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(Message.ClientRequest<?> request) {
        return client.submit(request);
    }

    @Override
    public ProtocolState state() {
        return client.connection().codec().state();
    }

    @Override
    public void subscribe(SessionListener listener) {
        client.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(SessionListener listener) {
        return client.unsubscribe(listener);
    }
}

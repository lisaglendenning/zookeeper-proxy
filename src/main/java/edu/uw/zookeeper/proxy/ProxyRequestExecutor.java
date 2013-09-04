package edu.uw.zookeeper.proxy;

import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;

public class ProxyRequestExecutor
        implements TaskExecutor<SessionOperation.Request<?>, Message.ServerResponse<?>> {

    public static ProxyRequestExecutor newInstance(
            Map<Long, ClientConnectionExecutor<?>> clients) {
        return new ProxyRequestExecutor(clients);
    }
    
    protected final Map<Long, ClientConnectionExecutor<?>> clients;

    public ProxyRequestExecutor(Map<Long, ClientConnectionExecutor<?>> clients) {
        this.clients = clients;
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            SessionOperation.Request<?> request) {
        Long sessionId = request.getSessionId();
        ClientConnectionExecutor<?> client = clients.get(sessionId);
        return client.submit(request);
    }
}

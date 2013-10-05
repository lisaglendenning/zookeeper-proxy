package edu.uw.zookeeper.proxy;

import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.client.MessageClientExecutor;

public class ProxyRequestExecutor
        implements TaskExecutor<SessionOperation.Request<?>, Message.ServerResponse<?>> {

    public static ProxyRequestExecutor newInstance(
            Map<Long, MessageClientExecutor<?>> clients) {
        return new ProxyRequestExecutor(clients);
    }

    protected final Map<Long, MessageClientExecutor<?>> clients;
    
    public ProxyRequestExecutor(Map<Long, MessageClientExecutor<?>> clients) {
        this.clients = clients;
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            SessionOperation.Request<?> request) {
        Long sessionId = request.getSessionId();
        MessageClientExecutor<?> client = clients.get(sessionId);
        if (client == null) {
            throw new RejectedExecutionException(String.valueOf(sessionId));
        }
        // hacky...
        return client.submit(((SessionRequest<?>) request).get());
    }
}

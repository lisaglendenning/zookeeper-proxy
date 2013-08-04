package edu.uw.zookeeper.proxy;

import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.Records;

public class ProxyRequestExecutor<C extends Connection<? super Message.ClientSession>> 
        implements TaskExecutor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> {

    public static <C extends Connection<? super Message.ClientSession>> ProxyRequestExecutor<C> newInstance(
            Map<Long, ClientConnectionExecutor<C>> clients) {
        return new ProxyRequestExecutor<C>(clients);
    }
    
    protected final Map<Long, ClientConnectionExecutor<C>> clients;

    public ProxyRequestExecutor(Map<Long, ClientConnectionExecutor<C>> clients) {
        this.clients = clients;
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<Records.Response>> submit(
            SessionOperation.Request<Records.Request> request) {
        Long sessionId = request.getSessionId();
        ClientConnectionExecutor<C> client = clients.get(sessionId);
        return client.submit(request);
    }
}

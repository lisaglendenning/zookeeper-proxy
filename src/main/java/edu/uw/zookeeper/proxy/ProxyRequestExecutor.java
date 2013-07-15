package edu.uw.zookeeper.proxy;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.TaskExecutor;

public class ProxyRequestExecutor<C extends Connection<? super Message.ClientSession>> 
        implements TaskExecutor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>,
        Function<Pair<Operation.ProtocolRequest<?>, Message.ServerResponse<?>>, Message.ServerResponse<Records.Response>> {

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
        return Futures.transform(client.submit(request), this);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public @Nullable Message.ServerResponse<Records.Response> apply(
            @Nullable Pair<Operation.ProtocolRequest<?>, Message.ServerResponse<?>> input) {
        return (input == null) ? null : (Message.ServerResponse<Records.Response>) input.second();
    }
}

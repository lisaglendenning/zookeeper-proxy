package edu.uw.zookeeper.proxy;

import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;

public class ProxyRequestExecutor<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> 
        implements TaskExecutor<SessionOperation.Request<?>, Message.ServerResponse<?>> {

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ProxyRequestExecutor<C> newInstance(
            Map<Long, ClientConnectionExecutor<C>> clients) {
        return new ProxyRequestExecutor<C>(clients);
    }
    
    protected final Map<Long, ClientConnectionExecutor<C>> clients;

    public ProxyRequestExecutor(Map<Long, ClientConnectionExecutor<C>> clients) {
        this.clients = clients;
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            SessionOperation.Request<?> request) {
        Long sessionId = request.getSessionId();
        ClientConnectionExecutor<C> client = clients.get(sessionId);
        return client.submit(request);
    }
}

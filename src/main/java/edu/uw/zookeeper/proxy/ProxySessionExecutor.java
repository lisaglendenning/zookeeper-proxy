package edu.uw.zookeeper.proxy;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.MessageClientExecutor;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.server.SessionExecutor;

@Listener(references = References.Strong)
public class ProxySessionExecutor implements SessionExecutor {

    public static ParameterizedFactory<Pair<? extends MessageClientExecutor<?>, ? extends PubSubSupport<Object>>, ProxySessionExecutor> factory() {
        return new ParameterizedFactory<Pair<? extends MessageClientExecutor<?>, ? extends PubSubSupport<Object>>, ProxySessionExecutor>() {
            @Override
            public ProxySessionExecutor get(
                    Pair<? extends MessageClientExecutor<?>, ? extends PubSubSupport<Object>> value) {
                return new ProxySessionExecutor(value.first(), value.second());
            }
        };
    }
    
    protected final PubSubSupport<Object> publisher;
    protected final MessageClientExecutor<?> client;
    
    public ProxySessionExecutor(
            MessageClientExecutor<?> client,
            PubSubSupport<Object> publisher) {
        this.client = client;
        this.publisher = publisher;
        
        client.subscribe(this);
        if (client.connection().state().compareTo(Connection.State.CONNECTION_CLOSING) >= 0) {
            client.unsubscribe(this);
        }
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
    public void subscribe(Object listener) {
        publisher.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(Object listener) {
        return publisher.unsubscribe(listener);
    }

    @Override
    public void publish(Object message) {
        publisher.publish(message);
    }
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(Message.ClientRequest<?> request) {
        return client.submit(request);
    }

    @Handler
    public void handleMessage(Message.ServerResponse<?> result) {
        if ((result.xid() == OpCodeXid.NOTIFICATION.xid()) 
                || (result.xid() == OpCodeXid.PING.xid())) {
            publish(result);
        }
    }

    @Handler
    public void handleTransition(Automaton.Transition<?> event) {
        if (event.to() == Connection.State.CONNECTION_CLOSED) {
            client.unsubscribe(this);
        }
    }
}

package edu.uw.zookeeper.proxy;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.MessageClientExecutor;

public class ProxyConnectExecutor implements TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> {

    public static ProxyConnectExecutor newInstance(
            Executor executor,
            ConcurrentMap<Long, Publisher> listeners,
            ConcurrentMap<Long, MessageClientExecutor<?>> clients,
            DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory) {
        return new ProxyConnectExecutor(executor, listeners, clients, clientFactory);
    }
    
    protected final Logger logger = LogManager.getLogger(getClass());
    protected final DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory;
    protected final ConcurrentMap<Long, Publisher> listeners;
    protected final ConcurrentMap<Long, MessageClientExecutor<?>> clients;
    protected final Executor executor;
    
    public ProxyConnectExecutor(
            Executor executor,
            ConcurrentMap<Long, Publisher> listeners,
            ConcurrentMap<Long, MessageClientExecutor<?>> clients,
            DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory) {
        this.executor = executor;
        this.listeners = listeners;
        this.clients = clients;
        this.clientFactory = clientFactory;
    }
    
    @Override
    public ListenableFuture<ConnectMessage.Response> submit(Pair<ConnectMessage.Request, Publisher> request) {
        return Futures.transform(
                clientFactory.get(request.first()),
                new ConnectTask(request.second()));
    }

    protected class ConnectTask implements AsyncFunction<MessageClientExecutor<?>, ConnectMessage.Response> {
        
        protected final Publisher listener;

        public ConnectTask(Publisher listener) {
            super();
            this.listener = listener;
        }

        @Override
        public ListenableFuture<ConnectMessage.Response> apply(
                MessageClientExecutor<?> input) throws Exception {
            return Futures.transform(input.session(), new ConnectedTask(input, listener));
        }
    }
    
    protected class ConnectedTask implements Function<ConnectMessage.Response, ConnectMessage.Response> {

        protected final Publisher listener;
        protected final MessageClientExecutor<?> client;
        
        public ConnectedTask(
                MessageClientExecutor<?> client,
                Publisher listener) {
            this.listener = listener;
            this.client = client;
        }

        @Override
        public ConnectMessage.Response apply(
                ConnectMessage.Response input) {
            logger.entry(input);
            if (input instanceof ConnectMessage.Response.Valid) {
                Long sessionId = input.getSessionId();
                Publisher prevPublisher = listeners.put(sessionId, listener);
                if (prevPublisher != null) {
                    // TODO
                    throw new UnsupportedOperationException();
                }
                MessageClientExecutor<?> prevClient = clients.put(sessionId, client);
                if (prevClient != null) {
                    // TODO
                    throw new UnsupportedOperationException();
                }
                
                new ConnectionListener(sessionId, listener, client);
            }
            listener.post(input);
            return logger.exit(input);
        }
    }
    
    protected class ConnectionListener {
        protected final Long sessionId;
        protected final Publisher publisher;
        protected final MessageClientExecutor<?> client;
        
        public ConnectionListener(
                Long sessionId, 
                Publisher publisher, 
                MessageClientExecutor<?> client) {
            this.sessionId = sessionId;
            this.publisher = publisher;
            this.client = client;
            
            client.register(this);
        }
        
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                listeners.remove(sessionId, publisher);
                clients.remove(sessionId, client);
                
                try {
                    client.unregister(this);
                } catch (IllegalArgumentException e) {}
            }
        }
        
        @Subscribe
        public void handleMessage(Message.ServerResponse<?> message) {
            publisher.post(message);
        }
    }
}

package edu.uw.zookeeper.proxy;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.ConnectMessage.Response;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.TaskExecutor;

public class ProxyConnectExecutor<V extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> implements TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> {

    public static <V extends ServerView.Address<? extends SocketAddress>, C extends Connection<? super Message.ClientSession>> ProxyConnectExecutor<V,C> newInstance(
            Executor executor,
            ConcurrentMap<Long, Publisher> listeners,
            ConcurrentMap<Long, ClientConnectionExecutor<C>> clients,
            EnsembleViewFactory<V, ServerViewFactory<ConnectMessage.Request, V, C>> clientFactory) {
        return new ProxyConnectExecutor<V,C>(executor, listeners, clients, clientFactory);
    }
    
    protected final EnsembleViewFactory<V, ServerViewFactory<ConnectMessage.Request, V, C>> clientFactory;
    protected final ConcurrentMap<Long, Publisher> listeners;
    protected final ConcurrentMap<Long, ClientConnectionExecutor<C>> clients;
    protected final Executor executor;
    
    public ProxyConnectExecutor(
            Executor executor,
            ConcurrentMap<Long, Publisher> listeners,
            ConcurrentMap<Long, ClientConnectionExecutor<C>> clients,
            EnsembleViewFactory<V, ServerViewFactory<ConnectMessage.Request, V, C>> clientFactory) {
        this.executor = executor;
        this.listeners = listeners;
        this.clients = clients;
        this.clientFactory = clientFactory;
    }
    
    @Override
    public ListenableFuture<ConnectMessage.Response> submit(Pair<ConnectMessage.Request, Publisher> request) {
        Promise<ConnectMessage.Response> promise = SettableFuturePromise.create();
        executor.execute(new ConnectionTask(executor, request, promise));
        return promise;
    }

    protected class ConnectionTask implements Runnable {
        
        protected final Executor executor;
        protected final Pair<ConnectMessage.Request, Publisher> request;
        protected final Promise<ConnectMessage.Response> promise;

        public ConnectionTask(Executor executor,
                Pair<ConnectMessage.Request, Publisher> request, Promise<Response> promise) {
            super();
            this.executor = executor;
            this.request = request;
            this.promise = promise;
        }

        @Override
        public void run() {
            try {
                ClientConnectionExecutor<C> client = clientFactory.get().get(request.first());
                Futures.addCallback(
                        client.session(), 
                        new ConnectedTask(request, client, promise), executor);
            } catch (Throwable t) {
                promise.setException(t);
            }
        }
        
    }
    
    protected class ConnectedTask implements FutureCallback<ConnectMessage.Response> {

        protected final Pair<ConnectMessage.Request, Publisher> request;
        protected final ClientConnectionExecutor<C> client;
        protected final Promise<ConnectMessage.Response> promise;
        
        public ConnectedTask(
                Pair<ConnectMessage.Request, Publisher> request,
                ClientConnectionExecutor<C> client,
                Promise<ConnectMessage.Response> promise) {
            this.request = request;
            this.client = client;
            this.promise = promise;
        }
        
        @Override
        public void onSuccess(ConnectMessage.Response response) {
            Publisher publisher = request.second();
            if (response instanceof ConnectMessage.Response.Valid) {
                Long sessionId = response.getSessionId();
                Publisher prevPublisher = listeners.put(sessionId, publisher);
                if (prevPublisher != null) {
                    // TODO
                }
                ClientConnectionExecutor<C> prevClient = clients.put(sessionId, client);
                if (prevClient != null) {
                    // TODO
                }
                
                new ConnectionListener(sessionId, publisher, client);
            }
            publisher.post(response);
            promise.set(response);
        }

        @Override
        public void onFailure(Throwable t) {
            promise.setException(t);
        }
    }
    
    protected class ConnectionListener {
        protected final Long sessionId;
        protected final Publisher publisher;
        protected final ClientConnectionExecutor<C> client;
        
        public ConnectionListener(
                Long sessionId, 
                Publisher publisher, 
                ClientConnectionExecutor<C> client) {
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

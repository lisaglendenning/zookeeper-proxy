package edu.uw.zookeeper.proxy;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.client.MessageClientExecutor;

public class ProxyConnectExecutor implements TaskExecutor<ConnectMessage.Request, ConnectMessage.Response> {

    public static ProxyConnectExecutor create(
            ConcurrentMap<Long, ProxySessionExecutor> sessions,
            DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory) {
        return new ProxyConnectExecutor(
                sessions, 
                clientFactory);
    }
    
    protected final Logger logger;
    protected final Map<Long, ProxySessionExecutor> executors;
    protected final DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory;

    public ProxyConnectExecutor(
            ConcurrentMap<Long, ProxySessionExecutor> executors,
            DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory) {
        this.logger = LogManager.getLogger(this);
        this.executors = executors;
        this.clientFactory = clientFactory;
    }
    
    @Override
    public ListenableFuture<ConnectMessage.Response> submit(ConnectMessage.Request request) {
        if (request instanceof ConnectMessage.Request.RenewRequest) {
            ProxySessionExecutor executor = executors.get(Long.valueOf(request.getSessionId()));
            if (executor != null) {
                return Futures.immediateFuture(ConnectMessage.Response.Valid.newInstance(executor.session(), request.getReadOnly(), request.legacy()));
            }
        }
        return Futures.transform(
                clientFactory.get(request),
                new ConnectTask());
    }

    protected class ConnectTask implements AsyncFunction<MessageClientExecutor<?>, ConnectMessage.Response> {
        
        public ConnectTask() {
            super();
        }

        @Override
        public ListenableFuture<ConnectMessage.Response> apply(
                MessageClientExecutor<?> input) throws Exception {
            return Futures.transform(
                    input.session(), 
                    new ConnectedTask(input));
        }
    }
    
    protected class ConnectedTask implements Function<ConnectMessage.Response, ConnectMessage.Response> {

        protected final MessageClientExecutor<?> client;
        
        public ConnectedTask(
                MessageClientExecutor<?> client) {
            this.client = client;
        }

        @Override
        public ConnectMessage.Response apply(
                ConnectMessage.Response input) {
            logger.entry(input);
            if (input instanceof ConnectMessage.Response.Valid) {
                ProxySessionExecutor session = ProxySessionExecutor.newInstance(client);
                ProxySessionExecutor prev = executors.put(input.getSessionId(), session);
                if (prev != null) {
                    // TODO
                    throw new UnsupportedOperationException();
                }
            }
            return logger.exit(input);
        }
    }
}

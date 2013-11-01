package edu.uw.zookeeper.proxy;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.client.MessageClientExecutor;
import edu.uw.zookeeper.server.AbstractConnectExecutor;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.SessionParametersPolicy;

public class ProxyConnectExecutor extends AbstractConnectExecutor {

    public static ProxyConnectExecutor defaults(
            Configuration configuration,
            ConcurrentMap<Long, ProxySessionExecutor> sessions,
            DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory) {
        DefaultSessionParametersPolicy policy = DefaultSessionParametersPolicy.fromConfiguration(configuration);
        ZxidReference lastZxid = new ZxidReference() {
            @Override
            public long get() {
                return Long.MAX_VALUE;
            }
        };
        return new ProxyConnectExecutor(
                policy, 
                lastZxid, 
                sessions, 
                clientFactory);
    }
    
    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final ConcurrentMap<Long, ProxySessionExecutor> sessions;
    protected final DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory;

    public ProxyConnectExecutor(
            SessionParametersPolicy policy,
            ZxidReference lastZxid,
            ConcurrentMap<Long, ProxySessionExecutor> sessions,
            DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory) {
        super(policy, lastZxid);
        this.sessions = sessions;
        this.clientFactory = clientFactory;
    }
    
    @Override
    public ListenableFuture<ConnectMessage.Response> submit(ConnectMessage.Request request) {
        return Futures.transform(
                clientFactory.get(request),
                new ConnectTask(),
                SAME_THREAD_EXECUTOR);
    }

    @Override
    protected Session put(Session session) {
        return get(session.id());
    }

    @Override
    protected ConcurrentMap<Long, ProxySessionExecutor> sessions() {
        return sessions;
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
                    new ConnectedTask(input),
                    SAME_THREAD_EXECUTOR);
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
                ProxySessionExecutor prev = sessions.put(input.getSessionId(), session);
                if (prev != null) {
                    // TODO
                    throw new UnsupportedOperationException();
                }
            }
            return logger.exit(input);
        }
    }
}

package edu.uw.zookeeper.proxy;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.bus.SyncBusConfiguration;
import net.engio.mbassy.bus.SyncMessageBus;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.client.MessageClientExecutor;
import edu.uw.zookeeper.server.AbstractConnectExecutor;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.SessionEvent;
import edu.uw.zookeeper.server.SessionParametersPolicy;

public class ProxyConnectExecutor extends AbstractConnectExecutor {

    public static ProxyConnectExecutor defaults(
            Configuration configuration,
            ConcurrentMap<Long, ProxySessionExecutor> sessions,
            DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory,
            ParameterizedFactory<Pair<? extends MessageClientExecutor<?>, ? extends PubSubSupport<Object>>, ? extends ProxySessionExecutor> sessionFactory) {
        DefaultSessionParametersPolicy policy = DefaultSessionParametersPolicy.fromConfiguration(configuration);
        @SuppressWarnings("rawtypes")
        PubSubSupport<Object> publisher = new SyncMessageBus<Object>(new SyncBusConfiguration());
        ZxidReference lastZxid = new ZxidReference() {
            @Override
            public long get() {
                return Long.MAX_VALUE;
            }
        };
        return new ProxyConnectExecutor(
                publisher, 
                policy, 
                lastZxid, 
                sessions, 
                clientFactory, 
                sessionFactory);
    }
    
    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final ConcurrentMap<Long, ProxySessionExecutor> sessions;
    protected final DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory;
    protected final ParameterizedFactory<Pair<? extends MessageClientExecutor<?>, ? extends PubSubSupport<Object>>, ? extends ProxySessionExecutor> sessionFactory;
    
    public ProxyConnectExecutor(
            PubSubSupport<? super SessionEvent> publisher,
            SessionParametersPolicy policy,
            ZxidReference lastZxid,
            ConcurrentMap<Long, ProxySessionExecutor> sessions,
            DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory,
            ParameterizedFactory<Pair<? extends MessageClientExecutor<?>, ? extends PubSubSupport<Object>>, ? extends ProxySessionExecutor> sessionFactory) {
        super(publisher, policy, lastZxid);
        this.sessions = sessions;
        this.clientFactory = clientFactory;
        this.sessionFactory = sessionFactory;
    }
    
    @Override
    public ListenableFuture<ConnectMessage.Response> submit(Pair<ConnectMessage.Request, ? extends PubSubSupport<Object>> request) {
        return Futures.transform(
                clientFactory.get(request.first()),
                new ConnectTask(request.second()),
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
        
        protected final PubSubSupport<Object> publisher;

        public ConnectTask(PubSubSupport<Object> publisher) {
            super();
            this.publisher = publisher;
        }

        @Override
        public ListenableFuture<ConnectMessage.Response> apply(
                MessageClientExecutor<?> input) throws Exception {
            return Futures.transform(
                    input.session(), 
                    new ConnectedTask(input, publisher),
                    SAME_THREAD_EXECUTOR);
        }
    }
    
    protected class ConnectedTask implements Function<ConnectMessage.Response, ConnectMessage.Response> {

        protected final PubSubSupport<Object> publisher;
        protected final MessageClientExecutor<?> client;
        
        public ConnectedTask(
                MessageClientExecutor<?> client,
                PubSubSupport<Object> listener) {
            this.publisher = listener;
            this.client = client;
        }

        @Override
        public ConnectMessage.Response apply(
                ConnectMessage.Response input) {
            logger.entry(input);
            if (input instanceof ConnectMessage.Response.Valid) {
                ProxySessionExecutor session = sessionFactory.get(Pair.create(client, publisher));
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

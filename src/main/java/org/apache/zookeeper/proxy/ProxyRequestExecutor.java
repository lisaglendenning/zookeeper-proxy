package org.apache.zookeeper.proxy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.RequestExecutorService;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.SessionConnectionState;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.client.ClientSessionConnection;
import org.apache.zookeeper.data.OpResult;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.data.Operations;
import org.apache.zookeeper.event.SessionResponseEvent;
import org.apache.zookeeper.server.AssignZxidProcessor;
import org.apache.zookeeper.server.SessionManager;
import org.apache.zookeeper.server.SessionRequestExecutor;
import org.apache.zookeeper.util.AutomataState;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.OptionalProcessor;
import org.apache.zookeeper.util.Pair;
import org.apache.zookeeper.util.Processor;
import org.apache.zookeeper.util.SettableTask;

import com.google.common.base.Objects;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class ProxyRequestExecutor extends SessionRequestExecutor {

    public static class Factory extends SessionRequestExecutor.Factory {

        public static Factory create(Provider<Eventful> eventfulFactory,
                ExecutorService executor, SessionManager sessions, Zxid zxid,
                Provider<ClientSessionConnection> clientFactory) {
            return new Factory(eventfulFactory, executor, sessions, zxid,
                    clientFactory);
        }

        protected final Provider<ClientSessionConnection> clientFactory;

        @Inject
        protected Factory(Provider<Eventful> eventfulFactory,
                ExecutorService executor, SessionManager sessions, Zxid zxid,
                Provider<ClientSessionConnection> clientFactory) {
            super(eventfulFactory, executor, sessions, zxid);
            this.clientFactory = clientFactory;
        }

        @Override
        protected RequestExecutorService newExecutor(long sessionId) {
            SessionConnectionState state = SessionConnectionState.create(
                    eventfulFactory.get(), State.CONNECTED);
            Session session = sessions().get(sessionId);
            return ProxyRequestExecutor
                    .create(eventfulFactory,
                            executor(),
                            session,
                            state,
                            getResponseProcessor(getSessionProcessor(sessionId,
                                    state)), getProxyProcessor(), clientFactory
                                    .get());
        }

        protected Processor<Pair<Operation.Request, Operation.Result>, Operation.Result> getProxyProcessor() {
            Processor<Operation.Response, Operation.Response> responseProcessor = OptionalProcessor
                    .create(AssignZxidProcessor.create(zxid));
            Processor<Pair<Operation.Request, Operation.Result>, Operation.Result> processor = ProxyResultProcessor
                    .create(responseProcessor);
            return processor;
        }
    }

    public static class ProxyResultProcessor
            implements
            Processor<Pair<Operation.Request, Operation.Result>, Operation.Result> {

        public static ProxyResultProcessor create(
                Processor<Operation.Response, Operation.Response> processor) {
            return new ProxyResultProcessor(processor);
        }

        protected Processor<Operation.Response, Operation.Response> processor;

        public ProxyResultProcessor(
                Processor<Operation.Response, Operation.Response> processor) {
            this.processor = processor;
        }

        @Override
        public Operation.Result apply(
                Pair<Operation.Request, Operation.Result> input)
                throws Exception {

            // unwrap backend response
            Operation.Result remoteResult = input.second();
            Operation.Response remoteResponse = remoteResult.response();
            if (remoteResponse instanceof Operation.CallResponse) {
                remoteResponse = ((Operation.CallResponse) remoteResponse)
                        .response();
            }

            // and wrap it back up
            Operation.Request localRequest = input.first();
            Operation.Response localResponse = processor.apply(remoteResponse);
            Operation.Result localResult = OpResult.create(localRequest,
                    localResponse);
            return localResult;
        }
    }

    public static enum ProxyRequestTaskState implements
            AutomataState<ProxyRequestTaskState> {
        INITIALIZING, EXECUTING, COMPLETED;

        @Override
        public boolean isTerminal() {
            switch (this) {
            case COMPLETED:
                return true;
            default:
                return false;
            }
        }

        @Override
        public boolean validTransition(ProxyRequestTaskState nextState) {
            if (this == nextState) {
                return true;
            }
            switch (this) {
            case INITIALIZING:
                return (nextState == EXECUTING);
            case EXECUTING:
                return (nextState == COMPLETED);
            default:
                return false;
            }
        }
    }

    protected class ProxyRequestTask extends
            SettableTask<Operation.Request, Operation.Result> implements
            Callable<ListenableFuture<Operation.Result>>,
            FutureCallback<Operation.Result> {

        protected ProxyRequestTaskState.Reference<ProxyRequestTaskState> state;

        public ProxyRequestTask(Operation.Request task) {
            super(task);
            this.state = ProxyRequestTaskState.Reference
                    .create(ProxyRequestTaskState.INITIALIZING);
        }
        
        public ProxyRequestTaskState state() {
            return state.get();
        }

        public ListenableFuture<Operation.Result> call() {
            SettableFuture<Operation.Result> future = future();
            if (state.compareAndSet(ProxyRequestTaskState.INITIALIZING,
                    ProxyRequestTaskState.EXECUTING)) {
                Operation.Request request = task();
                switch (request.operation()) {
                case PING: {
                    return apply(this);
                }
                case CLOSE_SESSION: {
                    Futures.addCallback(client.disconnect(), this);
                    break;
                }
                default: {
                    Futures.addCallback(
                            client.submit(Operations.Requests.unwrap(request)),
                            this);
                    break;
                }
                }
            }
            return future;
        }

        @Override
        public void onSuccess(Operation.Result result) {
            logger.debug("Success {}", result);
            state.compareAndSet(ProxyRequestTaskState.EXECUTING,
                    ProxyRequestTaskState.COMPLETED);

            if ((result.operation() == Operation.CLOSE_SESSION)
                    && !(result instanceof Operation.Error)) {
                apply(this);
            } else {
                SettableFuture<Operation.Result> future = future();
                try {
                    Operation.Result localResult = proxyProcessor().apply(
                            Pair.create(task(), result));
                    future.set(localResult);
                } catch (Exception e) {
                    onFailure(e);
                }
            }
            schedule();
        }

        @Override
        public void onFailure(Throwable t) {
            logger.debug("Failure {}", t);
            state.compareAndSet(ProxyRequestTaskState.EXECUTING,
                    ProxyRequestTaskState.COMPLETED);
            SettableFuture<Operation.Result> future = future();
            future.setException(t);
            schedule();
        }

        @Override
        public String toString() {
            String futureString = Objects.toStringHelper(future())
                    .add("isDone", future().isDone())
                    .toString();
            return Objects.toStringHelper(this)
                    .add("task", task())
                    .add("future", futureString)
                    .add("state", state())
                    .toString();
        }
    }

    public static ProxyRequestExecutor create(
            Provider<Eventful> eventfulFactory,
            ExecutorService executor,
            Session session,
            SessionConnectionState state,
            Processor<Operation.Request, Operation.Result> processor,
            Processor<Pair<Operation.Request, Operation.Result>, Operation.Result> proxyProcessor,
            ClientSessionConnection client) {
        return new ProxyRequestExecutor(eventfulFactory, executor, session,
                state, processor, proxyProcessor, client);
    }

    protected final BlockingQueue<ProxyRequestTask> pendingRequests;
    protected final Processor<Pair<Operation.Request, Operation.Result>, Operation.Result> proxyProcessor;
    protected ClientSessionConnection client;

    @Inject
    protected ProxyRequestExecutor(
            Provider<Eventful> eventfulFactory,
            ExecutorService executor,
            Session session,
            SessionConnectionState state,
            Processor<Operation.Request, Operation.Result> processor,
            Processor<Pair<Operation.Request, Operation.Result>, Operation.Result> proxyProcessor,
            ClientSessionConnection client) {
        super(eventfulFactory, executor, session, state, processor);
        this.pendingRequests = new LinkedBlockingQueue<ProxyRequestTask>();
        this.proxyProcessor = proxyProcessor;
        this.client = client;
        client.register(this);
        if (client.state() == SessionConnection.State.ANONYMOUS) {
            client.connect();
        }
    }

    protected Processor<Pair<Operation.Request, Operation.Result>, Operation.Result> proxyProcessor() {
        return proxyProcessor;
    }

    @Override
    public ListenableFuture<Operation.Result> call() throws Exception {
        scheduled.compareAndSet(true, false);

        synchronized (this) {
            ProxyRequestTask task = (ProxyRequestTask) requests.poll();
            while (task != null) {
                task.call();
                pendingRequests.put(task);
                task = (ProxyRequestTask) requests.poll();
            }
        }

        ListenableFuture<Operation.Result> future = null;
        synchronized (this) {
            ProxyRequestTask task = pendingRequests.peek();
            if (task != null) {
                if (task.future().isDone()) {
                    future = task.future();
                    pendingRequests.take();
                } 
            }
        }
        
        if (!requests().isEmpty() || !pendingRequests.isEmpty()) {
            schedule();
        }

        return future;
    }
    

    @Subscribe
    public void handleEvent(SessionResponseEvent event) {
        Operation.Response response = event.event();
        switch (response.operation()) {
        case NOTIFICATION:
            post(event);
            break;
        default:
            break;
        }
    }

    @Override
    protected SettableTask<Operation.Request, Operation.Result> newTask(
            Operation.Request request) {
        return new ProxyRequestTask(request);
    }
}

package edu.uw.zookeeper.proxy;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;


import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.SessionClient;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.server.ServerSessionRequestExecutor;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.SettableTask;

public class ProxyRequestExecutor extends ServerSessionRequestExecutor implements Runnable {

    public static ProxyRequestExecutor newInstance(
            Publisher publisher,
            ProxyServerExecutor executor,
            long sessionId,
            SessionClient client) throws IOException {
        return new ProxyRequestExecutor(publisher,
                executor,
                processor(executor, sessionId),
                sessionId,
                client);
    }

    protected class ProxyRequestTask extends
            SettableTask<Operation.SessionRequest, Operation.SessionReply>
            implements Runnable {

        protected final ListenableFuture<Operation.SessionReply> backendReply;

        protected ProxyRequestTask(Operation.SessionRequest task) throws Exception {
            super(task);
            Operation.SessionRequest backendRequest = executor().asRequestProcessor().apply(task);
            this.backendReply = client.get().submit(backendRequest);
        }
        
        public ListenableFuture<Operation.SessionReply> backendReply() {
            return backendReply;
        }
        
        public boolean isRunnable() {
            return (! future().isDone() && backendReply.isDone());
        }
        
        @Override
        public void run() {
            if (! isRunnable()) {
                return;
            }
            
            if (backendReply.isCancelled()) {
                future().cancel(false);
            } else {
                try {
                    Operation.SessionReply reply = backendReply.get();
                    // Translate backend reply to proxy reply
                    Operation.SessionRequest request = task();
                    Operation.SessionReply result;
                    switch (request.request().opcode()) {
                    case CLOSE_SESSION:
                        if (reply.reply() instanceof Operation.Error) {
                            result = executor().asReplyProcessor().apply(Pair.create(Optional.of(request), reply));
                        } else {
                            result = processor.apply(request);
                        }
                        break;
                    default:
                        result = executor().asReplyProcessor().apply(Pair.create(Optional.of(request), reply));
                        break;
                    }
                    future().set(result);
                } catch (Exception e) {
                    future().setException(e);
                }
            }
        }
    }

    protected final BlockingQueue<ProxyRequestTask> pending;
    protected final SessionClient client;

    protected ProxyRequestExecutor(
            Publisher publisher,
            ProxyServerExecutor executor,
            Processor<Operation.SessionRequest, Operation.SessionReply> processor,
            long sessionId,
            SessionClient client) throws IOException {
        super(publisher, executor, processor, sessionId);
        this.pending = new LinkedBlockingQueue<ProxyRequestTask>();
        this.client = client;
        client.get().register(this);
        if (client.get().state() == ProtocolState.ANONYMOUS) {
            client.get().connect();
        }
    }

    @Override
    public ProxyServerExecutor executor() {
        return (ProxyServerExecutor) executor;
    }

    @Override
    public ListenableFuture<Operation.SessionReply> submit(Operation.SessionRequest request) {
        switch (request.request().opcode()) {
        case PING:
            // Respond to pings immediately because the ordering doesn't matter
            return super.submit(request);
        default:
            break;
        }
        
        touch();
        
        ProxyRequestTask task;
        try {
            task = enqueueRequest(request);
        } catch (Exception e) {
            throw new RejectedExecutionException(e);
        }
        return task.future();
    }
    
    protected synchronized ProxyRequestTask enqueueRequest(Operation.SessionRequest request) throws Exception {
        // ordering constraint: queue order is the same as submit to backend order
        ProxyRequestTask task = new ProxyRequestTask(request);
        pending.add(task);
        task.backendReply().addListener(this, executor().executor()); // TODO: or sameThread?
        return task;
    }

    @Subscribe
    public void handleEvent(final Operation.SessionReply message) {
        if (message.reply() instanceof Operation.Response) {
            switch (((Operation.Response) message.reply()).opcode()) {
            case NOTIFICATION:
                execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Operation.SessionReply result = executor().asReplyProcessor().apply(Pair.create(Optional.<Operation.SessionRequest>absent(), message));
                            post(result);
                        } catch (Exception e) {
                            // TODO
                            Throwables.propagate(e);
                        }
                    }
                });
                break;
            default:
                break;
            }
        }
    }
    
    @Override
    public synchronized void execute(Runnable runnable) {
        run(); // flush
        super.execute(runnable);
    }

    @Override
    public synchronized void run() {
        // ordering constraint: order in pending queue is the same as order executed
        ProxyRequestTask task = pending.peek();
        while (task != null && task.isRunnable()) {
            super.execute(pending.poll());
            task = pending.peek();
        }
    }
}

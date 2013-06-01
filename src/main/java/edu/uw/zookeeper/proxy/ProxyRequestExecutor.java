package edu.uw.zookeeper.proxy;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;


import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.server.ServerSessionRequestExecutor;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class ProxyRequestExecutor extends ServerSessionRequestExecutor implements Runnable {

    public static ProxyRequestExecutor newInstance(
            Publisher publisher,
            ProxyServerExecutor executor,
            long sessionId,
            ClientProtocolExecutor client) throws IOException {
        return new ProxyRequestExecutor(publisher,
                executor,
                processor(executor, sessionId),
                sessionId,
                client);
    }

    protected class ProxyRequestTask extends
            PromiseTask<Operation.SessionRequest, Operation.SessionReply>
            implements Runnable {

        protected final ListenableFuture<Operation.SessionResult> backend;

        protected ProxyRequestTask(Operation.SessionRequest task) throws Exception {
            super(task, SettableFuturePromise.<Operation.SessionReply>create());
            this.backend = client.submit(executor().asRequestProcessor().apply(task.request()));
        }
        
        public ListenableFuture<Operation.SessionResult> backend() {
            return backend;
        }
        
        public boolean isRunnable() {
            return (! isDone() && backend().isDone());
        }
        
        @Override
        public void run() {
            if (! isRunnable()) {
                return;
            }
            
            if (backend().isCancelled()) {
                delegate().cancel(false);
            } else {
                try {
                    Operation.SessionResult result = backend().get();
                    // Translate backend reply to proxy reply
                    Operation.SessionRequest request = task();
                    Operation.SessionReply proxied;
                    switch (request.request().opcode()) {
                    case CLOSE_SESSION:
                        if (result.reply() instanceof Operation.Error) {
                            proxied = executor().asReplyProcessor().apply(Pair.create(Optional.of(request), result.reply()));
                        } else {
                            proxied = processor.apply(request);
                        }
                        break;
                    default:
                        proxied = executor().asReplyProcessor().apply(Pair.create(Optional.of(request), result.reply()));
                        break;
                    }
                    set(proxied);
                } catch (Exception e) {
                    setException(e);
                }
            }
        }
    }

    protected final BlockingQueue<ProxyRequestTask> pending;
    protected final ClientProtocolExecutor client;

    protected ProxyRequestExecutor(
            Publisher publisher,
            ProxyServerExecutor executor,
            Processor<Operation.SessionRequest, Operation.SessionReply> processor,
            long sessionId,
            ClientProtocolExecutor client) throws IOException {
        super(publisher, executor, processor, sessionId);
        this.pending = new LinkedBlockingQueue<ProxyRequestTask>();
        this.client = client;
        client.register(this);
        if (client.state() == ProtocolState.ANONYMOUS) {
            client.connect();
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
        return task;
    }
    
    protected synchronized ProxyRequestTask enqueueRequest(Operation.SessionRequest request) throws Exception {
        // ordering constraint: queue order is the same as submit to backend order
        ProxyRequestTask task = new ProxyRequestTask(request);
        pending.add(task);
        task.backend().addListener(this, executor().executor()); // TODO: or sameThread?
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

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
import edu.uw.zookeeper.protocol.SessionReplyWrapper;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
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
            ClientProtocolConnection client) throws IOException {
        return new ProxyRequestExecutor(publisher,
                executor,
                processor(executor, sessionId),
                new ProxyRequestProcessor(executor.xids()),
                new ProxyReplyProcessor(executor.zxids()),
                sessionId,
                client);
    }

    public static class ProxyRequestProcessor implements Processor<Operation.SessionRequest, Operation.SessionRequest> {

        public static ProxyRequestProcessor newInstance(
                Processor<Operation.Request, Operation.SessionRequest> delegate) {
            return new ProxyRequestProcessor(delegate);
        }
        
        protected final Processor<Operation.Request, Operation.SessionRequest> delegate;
        
        protected ProxyRequestProcessor(
                Processor<Operation.Request, Operation.SessionRequest> wrapper) {
            this.delegate = wrapper;
        }
        
        @Override
        public Operation.SessionRequest apply(Operation.SessionRequest input) throws Exception {
            return delegate.apply(input.request());
        }
    }
    
    public static class ProxyReplyProcessor implements Processor<Pair<Optional<Operation.SessionRequest>, Operation.SessionReply>, Operation.SessionReply> {

        public static ProxyReplyProcessor newInstance(
                Processor<Operation.Reply, Long> delegate) {
            return new ProxyReplyProcessor(delegate);
        }
        
        protected final Processor<Operation.Reply, Long> delegate;
        
        protected ProxyReplyProcessor(
                Processor<Operation.Reply, Long> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public Operation.SessionReply apply(Pair<Optional<Operation.SessionRequest>, Operation.SessionReply> input) throws Exception {
            Optional<Operation.SessionRequest> request = input.first();
            Operation.SessionReply reply = input.second();
            
            int xid;
            if (reply instanceof Operation.XidHeader) {
                xid = ((Operation.XidHeader)reply).xid();
            } else if (request.isPresent()){
                xid = request.get().xid();
            } else {
                throw new IllegalArgumentException(input.toString());
            }
            
            Operation.Reply payload = reply.reply();
            Long zxid = delegate.apply(payload);
            return SessionReplyWrapper.create(xid, zxid, payload);
        }
    }

    protected class ProxyRequestTask extends
            SettableTask<Operation.SessionRequest, Operation.SessionReply>
            implements Runnable {

        protected final ListenableFuture<Operation.SessionReply> backendReply;

        protected ProxyRequestTask(Operation.SessionRequest task) throws Exception {
            super(task);
            Operation.SessionRequest backendRequest = requestProcessor.apply(task);
            this.backendReply = client.submit(backendRequest);
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
                    Operation.SessionReply result = replyProcessor.apply(Pair.create(Optional.of(task()), reply));
                    future().set(result);
                } catch (Exception e) {
                    future().setException(e);
                }
            }
        }
    }

    protected final BlockingQueue<ProxyRequestTask> pending;
    protected final ProxyRequestProcessor requestProcessor;
    protected final ProxyReplyProcessor replyProcessor;
    protected final ClientProtocolConnection client;

    protected ProxyRequestExecutor(
            Publisher publisher,
            ProxyServerExecutor executor,
            Processor<Operation.SessionRequest, Operation.SessionReply> processor,
            ProxyRequestProcessor requestProcessor,
            ProxyReplyProcessor replyProcessor,
            long sessionId,
            ClientProtocolConnection client) throws IOException {
        super(publisher, executor, processor, sessionId);
        this.requestProcessor = requestProcessor;
        this.replyProcessor = replyProcessor;
        this.pending = new LinkedBlockingQueue<ProxyRequestTask>();
        this.client = client;
        client.register(this);
        if (client.state() == ProtocolState.ANONYMOUS) {
            client.connect();
        }
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
        task.backendReply().addListener(this, executor.executor()); // TODO: or sameThread?
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
                            Operation.SessionReply result = replyProcessor.apply(Pair.create(Optional.<Operation.SessionRequest>absent(), message));
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

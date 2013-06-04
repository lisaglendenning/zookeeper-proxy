package edu.uw.zookeeper.proxy;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;


import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ForwardingQueue;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.ServerSessionRequestExecutor;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.PublisherActor;
import edu.uw.zookeeper.util.TaskMailbox;

public class ProxyRequestExecutor extends ServerSessionRequestExecutor {

    public static ProxyRequestExecutor newInstance(
            Publisher publisher,
            ProxyServerExecutor executor,
            long sessionId,
            ClientProtocolExecutor client) {
        return new ProxyRequestExecutor(publisher,
                executor,
                processor(executor, sessionId),
                sessionId,
                client);
    }

    protected class SubmitActor extends AbstractActor<PromiseTask<Operation.SessionRequest, Operation.SessionReply>, Void> {

        protected SubmitActor() {
            this(ProxyRequestExecutor.this, 
                    TaskMailbox.<Operation.SessionReply, PromiseTask<Operation.SessionRequest, Operation.SessionReply>>newQueue(), 
                    newState());
        }
        
        protected SubmitActor(Executor executor,
                Queue<PromiseTask<Operation.SessionRequest, Operation.SessionReply>> mailbox,
                AtomicReference<edu.uw.zookeeper.util.Actor.State> state) {
            super(executor, mailbox, state);
        }

        @Override
        protected Void apply(PromiseTask<Operation.SessionRequest, Operation.SessionReply> input) throws Exception {
            // ordering constraint: queue order is the same as submit to backend order
            if (! input.isDone()) {
                Operation.Request request = executor().asRequestProcessor().apply(input.task().request());
                ListenableFuture<Operation.SessionResult> backend = client.submit(request);
                PendingTask task = new PendingTask(backend, input);
                pending.send(task);
            }
            return null;
        }
    }

    protected static class PendingTask extends
            PromiseTask<ListenableFuture<Operation.SessionResult>, Operation.SessionReply> {

        protected PendingTask(ListenableFuture<Operation.SessionResult> task, PromiseTask<Operation.SessionRequest, Operation.SessionReply> promise) throws Exception {
            super(task, promise);
        }
        
        public boolean isReady() {
            return (task().isDone() || isDone());
        }
        
        @SuppressWarnings("unchecked")
        public Operation.SessionRequest request() {
            return ((PromiseTask<Operation.SessionRequest, Operation.SessionReply>)delegate).task();
        }
    }
    
    protected static class PendingQueue extends ForwardingQueue<PendingTask> {

        protected final Queue<PendingTask> delegate;

        protected PendingQueue() {
            this(AbstractActor.<PendingTask>newQueue());
        }
        
        protected PendingQueue(Queue<PendingTask> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        protected Queue<PendingTask> delegate() {
            return delegate;
        }
        
        @Override
        public boolean isEmpty() {
            return peek() != null;
        }
        
        @Override
        public PendingTask peek() {
            PendingTask next = super.peek();
            if ((next != null) && next.isReady()) {
                return next;
            } else {
                return null;
            }
        }
        
        @Override
        public synchronized PendingTask poll() {
            PendingTask next = super.peek();
            if (next != null) {
                return super.poll();
            } else {
                return null;
            }
        }
        
        @Override
        public void clear() {
            PendingTask next;
            while ((next = super.poll()) != null) {
                next.cancel(true);
            }
        }
    }

    protected class PendingActor extends AbstractActor<PendingTask, Void> {

        protected PendingActor() {
            this(ProxyRequestExecutor.this, new PendingQueue(), newState());
        }
                
        protected PendingActor(
                Executor executor, 
                PendingQueue mailbox,
                AtomicReference<State> state) {
            super(executor, mailbox, state);
        }
        
        @Override
        public void send(PendingTask task) {
            super.send(task);
            task.task().addListener(this, executor);
        }

        @Subscribe
        public void handleSessionReply(Operation.SessionReply message) {
            if (Records.OpCodeXid.NOTIFICATION.xid() == message.xid()) {
                try {
                    flush(message);
                } catch (Exception e) {
                    // TODO
                    throw Throwables.propagate(e);
                }
            }
        }

        protected synchronized void flush(Operation.SessionReply message) throws Exception {
            // TODO: possible ordering issues here...
            // flush completed replies first
            runAll();
            Operation.SessionReply result = executor().asReplyProcessor().apply(Pair.create(Optional.<Operation.SessionRequest>absent(), message));
            post(result);
        }
        
        @Override
        protected boolean runEnter() {
            if (State.WAITING == state.get()) {
                schedule();
                return false;
            } else {
                return super.runEnter();
            }
        }

        @Override
        protected synchronized void runAll() throws Exception {
            super.runAll();
        }
        
        @Override
        protected Void apply(PendingTask input) throws Exception {
            if (input.isDone()) {
                return null;
            }
            
            if (input.task().isCancelled()) {
                input.cancel(true);
                return null;
            }
            
            try {
                Operation.SessionResult result = input.task().get();
                // Translate backend reply to proxy reply
                Operation.SessionRequest request = input.request();
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
                input.set(proxied);
            } catch (Exception e) {
                input.setException(e);
            }
            
            return null;
        }
    }

    protected final SubmitActor submitted;
    protected final PendingActor pending;
    protected final PublisherActor publish;
    protected final ClientProtocolExecutor client;

    protected ProxyRequestExecutor(
            Publisher publisher,
            ProxyServerExecutor executor,
            Processor<Operation.SessionRequest, Operation.SessionReply> processor,
            long sessionId,
            ClientProtocolExecutor client) {
        super(publisher, executor, processor, sessionId);
        this.pending = new PendingActor();
        this.submitted = new SubmitActor();
        this.publish = PublisherActor.newInstance(this, this);
        this.client = client;
        
        client.register(pending);
        if (client.state() == ProtocolState.ANONYMOUS) {
            client.connect();
        }
    }

    @Override
    public ProxyServerExecutor executor() {
        return (ProxyServerExecutor) executor;
    }

    @Override
    public ListenableFuture<Operation.SessionReply> submit(Operation.SessionRequest request,
            Promise<Operation.SessionReply> promise) {
        switch (request.request().opcode()) {
        case PING:
            // Respond to pings immediately because the ordering doesn't matter
            return super.submit(request, promise);
        default:
            break;
        }
        
        touch();
        
        PromiseTask<Operation.SessionRequest, Operation.SessionReply> task = PromiseTask.of(request);
        submitted.send(task);
        
        return task;
    }
}

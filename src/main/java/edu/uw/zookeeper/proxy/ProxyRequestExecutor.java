package edu.uw.zookeeper.proxy;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;


import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ForwardingQueue;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
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

public class ProxyRequestExecutor<C extends Connection<? super Message.ClientSession>> extends ServerSessionRequestExecutor {

    public static <C extends Connection<? super Message.ClientSession>> ProxyRequestExecutor<C> newInstance(
            Publisher publisher,
            ProxyServerExecutor<C> executor,
            long sessionId,
            ClientConnectionExecutor<C> client) {
        return new ProxyRequestExecutor<C>(publisher,
                executor,
                processor(executor, sessionId),
                sessionId,
                client);
    }

    protected class SubmitActor extends AbstractActor<PromiseTask<Message.ClientRequest, Message.ServerResponse>, Void> {

        protected SubmitActor() {
            super(ProxyRequestExecutor.this, 
                    TaskMailbox.<Message.ServerResponse, PromiseTask<Message.ClientRequest, Message.ServerResponse>>newQueue(), 
                    newState());
        }
        
        @Override
        protected Void apply(PromiseTask<Message.ClientRequest, Message.ServerResponse> input) throws Exception {
            // ordering constraint: queue order is the same as submit to backend order
            if (! input.isDone()) {
                Records.Request request = executor().asRequestProcessor().apply(input.task().request());
                ListenableFuture<Pair<Message.ClientRequest, Message.ServerResponse>> backend = client.submit(request);
                PendingTask task = new PendingTask(backend, input);
                pending.send(task);
            }
            return null;
        }
    }

    protected static class PendingTask extends
            PromiseTask<ListenableFuture<Pair<Message.ClientRequest, Message.ServerResponse>>, Message.ServerResponse> {

        protected PendingTask(ListenableFuture<Pair<Message.ClientRequest, Message.ServerResponse>> task, PromiseTask<Message.ClientRequest, Message.ServerResponse> promise) throws Exception {
            super(task, promise);
        }
        
        public boolean isReady() {
            return (task().isDone() || isDone());
        }
        
        @SuppressWarnings("unchecked")
        public Message.ClientRequest request() {
            return ((PromiseTask<Message.ClientRequest, Message.ServerResponse>)delegate).task();
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
            return (peek() == null);
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
            PendingTask next = peek();
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
        public void handleServerResponse(Message.ServerResponse message) {
            if (OpCodeXid.NOTIFICATION.xid() == message.xid()) {
                try {
                    flush(message);
                } catch (Exception e) {
                    // TODO
                    throw Throwables.propagate(e);
                }
            }
        }

        protected synchronized void flush(Message.ServerResponse message) throws Exception {
            // TODO: possible ordering issues here...
            // flush completed replies first
            runAll();
            Message.ServerResponse result = executor().asReplyProcessor().apply(Pair.create(Optional.<Message.ClientRequest>absent(), message));
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
                Pair<Message.ClientRequest, Message.ServerResponse> result = input.task().get();
                // Translate backend reply to proxy reply
                Message.ClientRequest request = input.request();
                Message.ServerResponse proxied;
                switch (request.request().opcode()) {
                case CLOSE_SESSION:
                    if (result.second() instanceof Operation.Error) {
                        proxied = executor().asReplyProcessor().apply(Pair.create(Optional.of(request), result.second()));
                    } else {
                        proxied = processor.apply(request);
                    }
                    break;
                default:
                    proxied = executor().asReplyProcessor().apply(Pair.create(Optional.of(request), result.second()));
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
    protected final ClientConnectionExecutor<C> client;

    protected ProxyRequestExecutor(
            Publisher publisher,
            ProxyServerExecutor<C> executor,
            Processor<Message.ClientRequest, Message.ServerResponse> processor,
            long sessionId,
            ClientConnectionExecutor<C> client) {
        super(publisher, executor, processor, sessionId);
        this.pending = new PendingActor();
        this.submitted = new SubmitActor();
        this.publish = PublisherActor.newInstance(this, this);
        this.client = client;
        
        client.register(pending);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ProxyServerExecutor<C> executor() {
        return (ProxyServerExecutor<C>) executor;
    }

    @Override
    public ListenableFuture<Message.ServerResponse> submit(Message.ClientRequest request,
            Promise<Message.ServerResponse> promise) {
        switch (request.request().opcode()) {
        case PING:
            // Respond to pings immediately because the ordering doesn't matter
            return super.submit(request, promise);
        default:
            break;
        }
        
        touch();
        
        PromiseTask<Message.ClientRequest, Message.ServerResponse> task = PromiseTask.of(request);
        submitted.send(task);
        
        return task;
    }
}

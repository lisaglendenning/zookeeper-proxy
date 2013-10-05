package edu.uw.zookeeper.proxy;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.client.MessageClientExecutor;
import edu.uw.zookeeper.protocol.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;

public class ProxyServerTaskExecutor extends ServerTaskExecutor {

    public static ProxyServerTaskExecutor newInstance(
            ExecutorService executor,
            DefaultsFactory<ConnectMessage.Request, ? extends ListenableFuture<? extends MessageClientExecutor<?>>> clientFactory) {
        ConcurrentMap<Long, Publisher> listeners = new MapMaker().makeMap();
        ConcurrentMap<Long, MessageClientExecutor<?>> clients = new MapMaker().makeMap();
        TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                ServerTaskExecutor.ProcessorExecutor.of(FourLetterRequestProcessor.newInstance());
        ProxyConnectExecutor connectExecutor = ProxyConnectExecutor.newInstance(
                executor, 
                listeners, 
                clients, 
                clientFactory);
        ProxyRequestExecutor sessionExecutor = ProxyRequestExecutor.newInstance(clients);
        return new ProxyServerTaskExecutor(
                listeners, 
                clients,
                anonymousExecutor, 
                connectExecutor,
                sessionExecutor);
    }
    
    protected final ConcurrentMap<Long, Publisher> listeners;
    protected final ConcurrentMap<Long, MessageClientExecutor<?>> clients;
    
    public ProxyServerTaskExecutor(
            ConcurrentMap<Long, Publisher> listeners,
            ConcurrentMap<Long, MessageClientExecutor<?>> clients,
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<SessionOperation.Request<?>, Message.ServerResponse<?>> sessionExecutor) {
        super(anonymousExecutor, connectExecutor, sessionExecutor);
        this.listeners = listeners;
        this.clients = clients;
    }
}

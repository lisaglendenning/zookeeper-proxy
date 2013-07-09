package edu.uw.zookeeper.proxy;


import java.net.SocketAddress;

import com.google.common.base.Optional;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ClientConnectionExecutorsService;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.Operation.Request;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.PingingClient;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.proxy.netty.NettyModule;
import edu.uw.zookeeper.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.ExpireSessionsTask;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.ServerConnectionListener;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.server.SessionParametersPolicy;
import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.ServiceApplication;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.TimeValue;

public enum ProxyApplicationModule implements ParameterizedFactory<RuntimeModule, Application> {
    INSTANCE;
    
    public static ProxyApplicationModule getInstance() {
        return INSTANCE;
    }

    public static String CHROOT_ARG = "chroot";
    public static ZNodeLabel.Path EMPTY_CHROOT = ZNodeLabel.Path.root();
    
    @Override
    public Application get(RuntimeModule runtime) {
        ServiceMonitor monitor = runtime.serviceMonitor();
        AbstractMain.MonitorServiceFactory monitorsFactory = AbstractMain.monitors(monitor);

        NettyModule netModule = NettyModule.newInstance(runtime);
        
        // Client
        TimeValue timeOut = ClientApplicationModule.TimeoutFactory.newInstance().get(runtime.configuration());
        ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory = ClientApplicationModule.codecFactory();
        ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> pingingFactory = 
                PingingClient.factory(timeOut, runtime.executors().asScheduledExecutorServiceFactory().get());
        ClientConnectionFactory<Operation.Request, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                monitorsFactory.apply(
                    netModule.clients().get(
                            codecFactory, pingingFactory).get());

        EnsembleView<ServerInetAddressView> ensemble = ClientApplicationModule.ConfigurableEnsembleViewFactory.newInstance().get(runtime.configuration());
        EnsembleViewFactory<ServerInetAddressView, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> ensembleFactory = EnsembleViewFactory.newInstance(clientConnections, ServerInetAddressView.class, ensemble, timeOut);
        ClientConnectionExecutorsService<PingingClient<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> clients = monitorsFactory.apply(
                ClientConnectionExecutorsService.newInstance(ensembleFactory));
        
        // Server
        ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>>> serverConnectionFactory = 
                netModule.servers().get(
                        ServerApplicationModule.codecFactory(),
                        ServerApplicationModule.connectionFactory());
        ServerInetAddressView address = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(runtime.configuration());
        ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections = 
                monitorsFactory.apply(serverConnectionFactory.get(address.get()));
        SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(runtime.configuration());
        ExpiringSessionManager sessions = ExpiringSessionManager.newInstance(runtime.publisherFactory().get(), policy);
        ExpireSessionsTask expires = monitorsFactory.apply(ExpireSessionsTask.newInstance(sessions, runtime.executors().asScheduledExecutorServiceFactory().get(), runtime.configuration()));
        AssignZxidProcessor zxids = AssignZxidProcessor.newInstance();
        AssignXidProcessor xids = AssignXidProcessor.newInstance();
        
        Arguments arguments = runtime.configuration().asArguments();
        if (! arguments.has(CHROOT_ARG)) {
            arguments.add(arguments.newOption(CHROOT_ARG, Optional.of("Path"), Optional.of(EMPTY_CHROOT.toString())));
        }
        arguments.parse();
        ZNodeLabel.Path chroot = ZNodeLabel.Path.of(arguments.getValue(CHROOT_ARG));
        ProxyServerExecutor<PingingClient<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> serverExecutor = EMPTY_CHROOT.equals(chroot)
                ? ProxyServerExecutor.newInstance(
                        runtime.executors().asListeningExecutorServiceFactory().get(), runtime.publisherFactory(), sessions, zxids, xids, clients)
                : ChrootedProxyServerExecutor.newInstance(
                        runtime.executors().asListeningExecutorServiceFactory().get(), runtime.publisherFactory(), sessions, zxids, xids, clients, chroot);
        ServerConnectionListener server = ServerConnectionListener.newInstance(serverConnections, serverExecutor, serverExecutor, serverExecutor);

        return ServiceApplication.newInstance(runtime.serviceMonitor());
    }
}

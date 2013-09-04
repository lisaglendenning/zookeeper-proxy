package edu.uw.zookeeper.proxy;


import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class Main extends ZooKeeperApplication.ForwardingApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected Main(Application delegate) {
        super(delegate);
    }

    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<Main, ProxyServerBuilder, MainBuilder> {
        
        public MainBuilder() {
            this(ProxyServerBuilder.defaults());
        }

        public MainBuilder(
                ProxyServerBuilder delegate) {
            super(delegate);
        }
        
        @Override
        public MainBuilder setRuntimeModule(RuntimeModule runtime) {
            return newInstance(TracingProxyServerBuilder.fromRuntimeModule(runtime));
        }

        @Override
        protected MainBuilder newInstance(ProxyServerBuilder delegate) {
            return new MainBuilder(delegate);
        }

        @Override
        protected Main doBuild() {
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new Main(ServiceApplication.newInstance(monitor));
        }
    }
}

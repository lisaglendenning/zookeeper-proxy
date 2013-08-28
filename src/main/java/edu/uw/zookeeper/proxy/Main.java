package edu.uw.zookeeper.proxy;


import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.common.Configuration;

public class Main extends DefaultMain {

    public static void main(String[] args) {
        DefaultMain.main(args, ConfigurableApplicationFactory.newInstance(Main.class));
    }

    public Main(Configuration configuration) {
        super(ProxyApplicationModule.factory(), configuration);
    }
}

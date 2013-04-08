Simple ZooKeeper proxy based on http://github.com/lisaglendenning/zookeeper-lite.

# Quickstart

> $ mvn exec:java -Dexec.mainClass="org.apache.zookeeper.proxy.netty.NettyProxyMain" -Dexec.args="--help" -Dexec.classpathScope=test
> Usage: org.apache.zookeeper.proxy.netty.NettyProxyMain [--address=ServerAddress] [--clientAddress=ClientAddress] [--clientPort=ClientPort] [--help] [--port=ServerPort] [--prefs-path=PATH] [--prefs-root=[user,system]]

# Building

Both zookeeper-proxy and zookeeper-lite are Maven projects. Build zookeeper-lite first and install it into your local Maven repository.

Both zookeeper-proxy and zookeeper-lite use SL4J for logging. Apache Log4J is used as the SL4J backend in test scope.

# Running

1. First, start a regular ZooKeeper standalone server or ensemble.
2. Then, start the ZooKeeper proxy. Specify the backend server network address with the --address=HOSTNAME and --port=PORT command line arguments. Specify the network address that clients will connect to with the --clientAddress and --clientPort command line arguments. The default client address is */2181.
3. Finally, connect to the proxy with your choice of ZooKeeper client.

# Overview

Simple ZooKeeper server proxy based on [zookeeper-lite](http://github.com/lisaglendenning/zookeeper-lite).

## Quickstart

<pre>
$ mvn exec:java -Dexec.classpathScope=test -Dexec.mainClass="edu.uw.zookeeper.proxy.netty.NettyProxyMain" -Dexec.args="--help"
Usage: edu.uw.zookeeper.proxy.netty.NettyProxyMain [--address=ServerAddress] [--clientAddress=ClientAddress] [--clientPort=ClientPort] [--help] [--port=ServerPort]
</pre>

## Building

Both zookeeper-proxy and zookeeper-lite are Maven projects. Build zookeeper-lite first and install it into your local Maven repository.

Both zookeeper-proxy and zookeeper-lite use SLF4J for logging. Apache Log4J2 is used as the SLF4J backend in test scope.

## Running

1. First, start a vanilla ZooKeeper standalone server or ensemble.
2. Then, start the ZooKeeper proxy. Specify the backend server network address with the ``--address=HOSTNAME`` and ``--port=PORT`` command line arguments. Specify the network address that clients will connect to with the ``--clientAddress`` and ``--clientPort`` command line arguments. The default client address is ``*/2181``.
3. Finally, connect to the proxy with your choice of ZooKeeper client.

## Overview

For each client session, the proxy creates a corresponding session with the backend server. All data operations within a session are forwarded to the backend server for execution. The proxy rewrites the backend zxid with its own zxid.

Clients are handled concurrently and requests are executed asynchronously.

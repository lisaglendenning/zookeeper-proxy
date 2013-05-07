#!/usr/bin/env python
# Author: Lisa Glendenning <lglenden@cs.washington.edu>
# Script front end to edu.uw.zookeeper.proxy.netty.Main

import sys, os, shlex

ZOOKEEPER_PROXY_PREFIX = os.environ['ZOOKEEPER_PROXY_PREFIX'] \
    if 'ZOOKEEPER_PROXY_PREFIX' in os.environ \
    else os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
ZOOKEEPER_PROXY_LIB = os.environ['ZOOKEEPER_PROXY_LIB'] \
    if 'ZOOKEEPER_PROXY_LIB' in os.environ \
    else os.path.join(ZOOKEEPER_PROXY_PREFIX, 'lib')
ZOOKEEPER_PROXY_ETC = os.environ['ZOOKEEPER_PROXY_ETC'] \
    if 'ZOOKEEPER_PROXY_ETC' in os.environ \
    else os.path.join(ZOOKEEPER_PROXY_PREFIX, 'etc')

ZOOKEEPER_PROXY_LOG_CONFIG = ""
if 'ZOOKEEPER_PROXY_LOG_CONFIG' in os.environ:
    ZOOKEEPER_PROXY_LOG_CONFIG = os.environ['ZOOKEEPER_PROXY_LOG_CONFIG']
else:
    for f in 'log4j2-test.xml', 'log4j2.xml':
        log_config_file = os.path.join(ZOOKEEPER_PROXY_ETC, f)
        if os.path.isfile(log_config_file):
            ZOOKEEPER_PROXY_LOG_CONFIG="-Dlog4j.configurationFile=%s" % log_config_file
            break

JAVA = 'java'
if 'JAVA_HOME' in os.environ:
    JAVA = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java')
JAVA_CLASSPATH = os.path.join(ZOOKEEPER_PROXY_LIB, "*")
JAVA_MAIN = "edu.uw.zookeeper.proxy.netty.Main"
JAVA_ARGS = ['-classpath', JAVA_CLASSPATH]
if len(ZOOKEEPER_PROXY_LOG_CONFIG) > 0:
    JAVA_ARGS.append(ZOOKEEPER_PROXY_LOG_CONFIG)
if 'JAVA_ARGS' in os.environ:
    JAVA_ARGS.extend(shlex.split(os.environ['JAVA_ARGS']))

def main(argv):
    args = [JAVA] + JAVA_ARGS
    args.append(JAVA_MAIN)
    args.extend(argv[1:])
    os.execvp(JAVA, args)

if __name__ == "__main__":
    main(sys.argv)

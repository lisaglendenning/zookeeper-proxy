#!/usr/bin/env python

# Author: Lisa Glendenning <lglenden@cs.washington.edu>
# Script front end to edu.uw.zookeeper.proxy.Main

import sys, os, shlex

def main(argv, environ):
    ZOOKEEPER_PROXY_PREFIX = environ.get(
                     'ZOOKEEPER_PROXY_PREFIX',
                     os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    ZOOKEEPER_PROXY_LIB = environ.get(
                     'ZOOKEEPER_PROXY_LIB',
                     os.path.join(ZOOKEEPER_PROXY_PREFIX, 'lib'))
    ZOOKEEPER_PROXY_ETC = environ.get(
                     'ZOOKEEPER_PROXY_ETC',
                     os.path.join(ZOOKEEPER_PROXY_PREFIX, 'etc'))
    
    ZOOKEEPER_PROXY_LOG_CONFIG = environ.get('ZOOKEEPER_PROXY_LOG_CONFIG')
    if not ZOOKEEPER_PROXY_LOG_CONFIG:
        for f in 'log4j2-test.xml', 'log4j2.xml':
            log_config_file = os.path.join(ZOOKEEPER_PROXY_ETC, f)
            if os.path.isfile(log_config_file):
                ZOOKEEPER_PROXY_LOG_CONFIG = \
                    "-Dlog4j.configurationFile=%s" % log_config_file
                break
    
    JAVA = os.path.join(environ['JAVA_HOME'], 'bin', 'java') \
            if 'JAVA_HOME' in environ else 'java'
    
    JAVA_CLASSPATH = os.path.join(ZOOKEEPER_PROXY_LIB, "*")
    JAVA_MAIN = "edu.uw.zookeeper.proxy.Main"
    JAVA_ARGS = ['-classpath', JAVA_CLASSPATH]
    if ZOOKEEPER_PROXY_LOG_CONFIG:
        JAVA_ARGS.append(ZOOKEEPER_PROXY_LOG_CONFIG)
    if 'JAVA_ARGS' in environ:
        JAVA_ARGS.extend(shlex.split(environ['JAVA_ARGS']))

    args = [JAVA] + JAVA_ARGS
    args.append(JAVA_MAIN)
    args.extend(argv[1:])
    os.execvp(JAVA, args)

if __name__ == "__main__":
    main(sys.argv, os.environ)

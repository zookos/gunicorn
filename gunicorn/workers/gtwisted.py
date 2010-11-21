# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.

import errno
import socket
import sys

from twisted.web.server import Site
from twisted.web.wsgi import WSGIResource
from twisted.internet import base, fdesc, reactor, task, tcp, unix
from twisted.internet import address, interfaces, defer, main
from twisted.python import failure, log, reflect, threadpool
from zope.interface import implements

from gunicorn.workers.base import Worker

class Port(base.BasePort):
    addressFamily = socket.AF_INET
    socketType = socket.SOCK_STREAM

    implements(interfaces.IListeningPort)

    transport = tcp.Server
    sessionno = 0
    _realPortNumber = 0

    def __init__(self, skt, factory, worker, reactor=None):
        base.BasePort.__init__(self, reactor=reactor)
        self.worker = worker
        self.socket = skt
        self.fileno = self.socket.fileno
        self.factory = factory
        self.factory.doStart()
        self.connected = True
        self.numberAccepts = worker.cfg.worker_connections

        if worker.socket.FAMILY == socket.AF_UNIX:
            self.transport = unix.Server
            self.addressFamily = socket.AF_UNIx

    def startListening(self):
        self.startReading()

    def _buildAddr(self, (host, port)):
        if self.worker.socket.FAMILY == socket.AF_UNIX:
            return address.UNIXAddress(name)
        return address._ServerFactoryIPv4Address('TCP', host, port)

    def doRead(self):
        try:
            numAccepts = self.numberAccepts
            for i in range(numAccepts):
                if self.disconnecting or not self.worker.alive:
                    return

                # If our parent changed then we shut down.
                if self.worker.ppid != os.getppid():
                    log.msg("Parent changed, shutting down: %s" % self.worker)
                    return

                try:
                    skt, addr = self.socket.accept()
                except socket.error, e:
                    if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                        break
                    elif e.args[0] == errno.EPERM:
                        continue
                    elif e[0] not in (errno.EMFILE, errno.ENOBUFS,
                            errno.ENFILE, errno.ENOMEM, errno.ECONNABORTED):
                        log.msg("could not accept connection")
                        break
                    raise
                
                fdesc._setCloseOnExec(skt.fileno())
                protocol = self.factory.buildProtocol(self._buildAddr(addr))
                if protocol is None:
                    skt.close()
                    continue
                s = self.sessionno
                self.sessionno = s+1
                transport = self.transport(skt, protocol, addr, self, s, 
                        self.reactor)
                transport = self._preMakeConnection(transport)
                protocol.makeConnection(transport)
        except:
            log.deferr()

    def _preMakeConnection(self, transport):
        return transport

    def loseConnection(self, connDone=failure.Failure(main.CONNECTION_DONE)):
        self.disconnecting = True
        self.worker.alive = False
        self.stopReading()
        if self.connected:
            self.deferred = deferLater(
                    self.reactor, 0, self.connectionLost, connDone)
            return self.deferred
    stopListening = loseConnection

    def connectionLost(self, reason):
        base.BasePort.connectionLost(self, reason)
        self.connected = False
        try:
            self.factory.doStop()
        finally:
            self.disconnecting = False

    def logPrefix(self):
        return reflect.qual(self.__class__)

class TwistedWorker(Worker):

    def handle_exit(self, *args):
        self.alive = False

    def run(self):
        self.socket.setblocking(0)

        resource = WSGIResource(reactor, reactor.getThreadPool(), self.wsgi)
        factory = Site(resource)
        p = Port(self.socket, factory, self, reactor)
        p.startListening()
        
        # notify loop
        l = task.LoopingCall(self.notify)
        l.start(self.timeout)

        # start the reactor
        reactor.run()


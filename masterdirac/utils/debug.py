DEBUG = True
import time
import os
import os.path
import sys

import pickle
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
import SocketServer
import struct
import socket
from multiprocessing import Process, Event
import threading
import sys
import signal

import static
import boto
import boto.utils
from boto.s3.key import Key

import select
import errno

"""
Below is lightly modified from http://docs.python.org/2/howto/logging-cookbook.html#logging-cookbook
"""
#global variable to kill stream handler from server
damn_global = 0
class LogRecordStreamHandler(SocketServer.StreamRequestHandler):
    """Handler for a streaming logging request.
    This basically logs the record using whatever logging policy is
    configured locally.
    """
    def handle(self):
        """
        Handle multiple requests - each expected to be a 4-byte length,
        followed by the LogRecord in pickle format. Logs the record
        according to whatever policy is configured locally.
        """
        global damn_global
        while not damn_global:
            chunk = self.connection.recv(4)
            if len(chunk) < 4:
                break
            slen = struct.unpack('>L', chunk)[0]
            chunk = self.connection.recv(slen)
            while len(chunk) < slen:
                chunk = chunk + self.connection.recv(slen - len(chunk))
            obj = self.unPickle(chunk)
            record = logging.makeLogRecord(obj)
            self.handleLogRecord(record)

    def unPickle(self, data):
        return pickle.loads(data)

    def handleLogRecord(self, record):
        # if a name is specified, we use the named logger rather than the one
        # implied by the record.
        if self.server.logname is not None:
            name = self.server.logname
        else:
            name = record.name
        logger = logging.getLogger(name)
        # N.B. EVERY record gets logged. This is because Logger.handle
        # is normally called AFTER logger-level filtering. If you want
        # to do filtering, do it at the client end to save wasting
        # cycles and network bandwidth!
        logger.handle(record)

class LogRecordSocketReceiver(SocketServer.ThreadingTCPServer):
    """
    Simple TCP socket-based logging receiver suitable for testing.
    """
    allow_reuse_address = 1
    def __init__(self, host='localhost', port=logging.handlers.DEFAULT_TCP_LOGGING_PORT, handler=LogRecordStreamHandler,doneEvent=None):
        SocketServer.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.abort = 0
        self.timeout = 1
        self.logname = None
        self.doneEvent=doneEvent

    def serve_until_stopped(self):
        import select
        cont = self.doneEvent.is_set()
        while cont:
            try:
                rd, wr, ex = select.select([self.socket.fileno()], [], [],  self.timeout)
                cont = self.doneEvent.is_set()
                if rd:
                    self.handle_request()
            except select.error,v:
                #interrupt during select, ignore
                if v[0] != errno.EINTR: raise
                else: break
        #set global variable to end any running handlers
        #I am not proud of this
        global damn_global
        damn_global = 1
        for handler in logging.getLogger('').handlers:
            if isinstance( handler, S3TimedRotatatingFileHandler):
                handler.doRollover()
        self.doneEvent.set()

class S3TimedRotatatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, backupCount=0, encoding=None, delay=False, utc=False, bucket='diraclog'):
        TimedRotatingFileHandler. __init__(self, filename, when, interval, backupCount, encoding, delay, utc)
        self.bucket = bucket

    def doRollover(self):
        """
        do a rollover; in this case, a date/time stamp is appended to the filename
        when the rollover happens.  However, you want the file to be named for the
        start of the interval, not the current time.  If there is a backup count,
        then we have to get a list of matching filenames, sort them and remove
        the one with the oldest suffix.
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        # get the time that this sequence started at and make it a TimeTuple
        currentTime = int(time.time())
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)
        dfn = self.baseFilename + "." + time.strftime(self.suffix, timeTuple)
        if os.path.exists(dfn):
            os.remove(dfn)
        # Issue 18940: A file may not have been created if delay is True.
        if os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, dfn)
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)
        #if not self.delay:
        self.stream = self._open()
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        #If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                    addend = -3600
                else:           # DST bows out before next rollover, so we need to add an hour
                    addend = 3600
                newRolloverAt += addend
        self.rolloverAt = newRolloverAt
        #this is the new stuff
        #copy to s3 the old file
        self.pushToS3( dfn )

    def pushToS3(self, filename):
        conn = boto.connect_s3()
        bucket = conn.get_bucket(self.bucket)
        k = Key(bucket)
        k.key = os.path.split(filename)[1]
        k.set_contents_from_filename(filename, reduced_redundancy=True)

def startLogger():
    import os, os.path
    import logging
    import masterdirac.models.systemdefaults as sys_def
    config =  sys_def.get_system_defaults('loggingserver', 'Master')
    log_dir = config['directory']
    LOG_FILENAME = config[ 'log_filename' ]
    if LOG_FILENAME == 'None':
        md =  boto.utils.get_instance_metadata()
        LOG_FILENAME = md['instance-id'] + '.log'
    bucket = config[ 'bucket']
    interval_type = config[ 'interval_type' ]
    interval = int(config['interval'])
    log_format = config['log_format']
    port = config[ 'port' ]
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    handler = S3TimedRotatatingFileHandler(os.path.join(log_dir,LOG_FILENAME),
                    when=interval_type, interval = interval, bucket=bucket)

    doneEvent = threading.Event()
    doneEvent.set()
    tcpserver = LogRecordSocketReceiver(doneEvent=doneEvent, port=int(port))
    def shutdownHandler(msg,evt):
        logging.getLogger('logging.SIGHANDLER').critical("Shutdown handler activated")
        if evt.is_set():#only want to do this once, if it is clear then it is shutting down
            evt.clear()
        sys.exit(0)


def initLogging():
    import masterdirac.models.systemdefaults as sys_def
    config =  sys_def.get_system_defaults('logging', 'Master')
    log_format = config['log_format']
    es_name = config[ 'external_server_name']
    es_port = config[ 'external_server_port']
    es_level = int(config[ 'external_server_level' ])

    is_name = config[ 'internal_server_name']
    is_port = config['internal_server_port']
    is_level = config['internal_server_level']

    boto_level = config[ 'boto_level']
    stdout_level =config[ 'stdout_level']

    botoLogger = logging.getLogger('boto')
    botoLogger.setLevel(int(boto_level))

    formatter = logging.Formatter(log_format)
    rootLogger = logging.getLogger('')
    rootLogger.setLevel(logging.DEBUG)

    if es_name !='None':
        print "In external server", es_level
        server = es_name
        port = es_port
        server_level = es_level
        socketHandler = logging.handlers.SocketHandler(server, int(port))
        socketHandler.setLevel(int(server_level))
        socketHandler.setFormatter(formatter)
        rootLogger.addHandler(socketHandler)
    if is_name !='None':
        print "In Internal server", is_level
        server = is_name
        port = is_port
        server_level = is_level
        socketHandler = logging.handlers.SocketHandler(server, int(port))
        socketHandler.setLevel(int(server_level))
        socketHandler.setFormatter(formatter)
        rootLogger.addHandler(socketHandler)
    if stdout_level != 'None':
        print "in stdout", stdout_level
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(int(stdout_level))
        ch.setFormatter(formatter)
        rootLogger.addHandler(ch)

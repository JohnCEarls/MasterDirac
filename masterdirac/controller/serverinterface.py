import json
from boto.sqs.message import Message
import logging
import boto.ec2
import boto.sqs
import collections
import masterdirac.models.worker as wkr_mdl

import masterdirac.models.server as svr_mdl

class ServerInterface(object):
    def __init__(self, init_message,master_name):
        self.name = init_message['name']
        self._master_name = master_name
        self.cluster_name = init_message['cluster-name']
        self.command_q = init_message['command']
        self.response_q = init_message['response']
        self.zone = init_message['zone']
        self.region = self.zone[:-1]
        self.instance_id = init_message['instance-id']
        self._unique = "%s-%s" % (self.name, self.instance_id)
        self.logger = logging.getLogger(self._unique)
        self.status_queue = collections.deque()
        self._terminated = False
        self._run_id = None
        self._worker_id = None
        self._status = None

    @property
    def conn(self):
        return boto.ec2.connect_to_region( self.region )

    @property
    def instance(self):
        inst = self.conn.get_only_instances(instance_ids=[self.instance_id])
        return inst[0]

    def get_responses( self ):
        """
        Grabs all responses and put the dicts in a status_queue
        """
        conn = boto.sqs.connect_to_region( 'us-east-1' )
        rq = conn.get_queue( self.response_q )
        responses = False
        if rq is not None:
            messages = rq.get_messages(10)
            for message in messages:
                my_mess = message.get_body()
                self.status_queue.append(json.loads(my_mess))
                rq.delete_message(message)
                responses = True
        return responses

    @property
    def worker_id(self):
        if self._worker_id is None:
            try:
                wm = wkr_mdl.get_ANWorker( cluster_name = self.cluster_name )
                if len(wm) > 0: 
                    self._worker_id = wm[0]['worker_id']
            except:
                self.logger.exception("Unable to get worker_model for %s" % self.cluster_name)
        if self._worker_id is None:
            try:
                self._terminated = True
                self.terminate()
            except:
                self.logger.exception("Inconsistent state")
        return self._worker_id

    @property
    def worker_model(self):
        return wkr_mdl._get_ANWorker( worker_id = self.worker_id )

    @property
    def status(self):
        if self._status is None:
            self._status =svr_mdl.get_status( self.cluster_name, self.server_id )
        return self._status

    def set_status(self, status):
        try:
            svr_mdl.update_ANServer( self.cluster_name, self.server_id, status)
        except:
            svr_mdl.insert_ANServer( self.cluster_name, self.server_id, status)
        self._status = status

    def refresh_status(self):
        self._status = None
        return self.status

    def _send_command( self, message):
        """
        Sends an arbitrary message to this gpu's command queue
        """
        conn = boto.sqs.connect_to_region( 'us-east-1' )
        cq = conn.get_queue( self.command_q )
        if cq is not None:
            cq.write( Message(body=message) )
            return True
        else:
            self.logger.warning("Unable to connect to [%s]" % self.command_q)
            return False

    def restart(self):
        self._restart()

    def terminate(self):
        self.logger.warning("Sending term signal")
        term_mess = {}
        term_mess['message-type'] = 'termination-notice'
        self._send_command(json.dumps(term_mess))

    def is_terminated(self):
        return self._terminated

    @property
    def terminated(self):
        return self.status == svr_mdl.TERMINATED

    def delete_queues( self ):
        try:
            conn = boto.sqs.connect_to_region( 'us-east-1' )
            rq = conn.get_queue( self.response_q )
            conn.delete_queue( rq )
        except Exception as e:
            self.logger.error("Attempted to delete %s" % self.response_q )
            self.logger.exception("ST:")
        try:
            conn = boto.sqs.connect_to_region( 'us-east-1' )
            rq = conn.get_queue( self.command_q )
            conn.delete_queue( rq )
        except Exception as e:
            self.logger.error("Attempted to delete %s" % self.response_q )
            self.logger.exception("ST")

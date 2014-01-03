import json
import boto
import boto.utils
import boto.sqs
from boto.sqs import Message
import logging

class ServerInterface(object):
    def __init__(self, init_message):
        self.name = init_message['name']
        self.command_q = init_message['command']
        self.response_q = init_message['response']
        self.zone = init_message['zone']
        self.region = self.zone[:-1]
        self.instance_id = init_message['instance-id']
        self._unique = "%s-%s" % (self.name, self.instance_id)
        self.logger = logging.getLogger(self._unique)

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
        if rq is not None:
            messages = rq.get_messages(10)
            for message in messages:
                my_mess = message.get_body()
                self.status_queue.append(json.loads(my_mess))
                rq.delete_message(message)

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

    def terminate(self):
        self.logger.warning("Sending term signal")
        term_mess = {}
        term_mess['message-type'] = 'termination-notice'
        self._send_command(json.dumps(term_mess))

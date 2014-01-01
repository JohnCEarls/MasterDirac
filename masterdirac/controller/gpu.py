import json
import boto
import boto.utils
import boto.sqs
from boto.sqs import Message
import logging
from collections import deque
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
        self.status_queue = dequeue()

    def send_init(self,  result_sqs, result_sqs, source_s3, result_s3,
           data_settings, sample_block_size, pairs_block_size, nets_block_size, 
           heartbeat_interval):
        self.logger.info("Initializing gpudirac-server" )
        gpu_message = {'message-type':'init-settings',
                 'result-sqs': result_sqs,
                 'source-sqs': source_sqs,
                 'source-s3': source_s3,
                 'result-s3': result_s3,
                 'data-settings' : data_settings,
                 'gpu-id': gpu_id,
                 'sample-block-size': sample_block_size,
                 'nets-block-size': nets_block_size,
                 'pairs-block-size': pairs_block_size,
                 'hearbeat-interval': heartbeat_interval
                }
        js_mess = json.dumps( gpu_message )
        self.logger.debug("GPUInit message [%s]" % js_mess )
        return self._send_command( js_mess )

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

    def get_response( self ):
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




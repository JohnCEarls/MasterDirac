import json
import boto
import boto.utils
import boto.sqs
from boto.sqs import Message
import logging
from collections import deque
from serverinterface import ServerInterface

class Interface(ServerInterface):
    def __init__(self, init_message):
        super( Interface, self ).__init__(init_message )
        self.logger = logging.getLogger(self._unique)
        self.logger.info("Data Interface created")
        self.num_nodes = init_message['num-nodes']

   def send_init(self, aws_locations, source_files, network_settings, 
                    block_sizes, gpu_mem_max):
        self._aws_locations = aws_locations
        data_sqs_queue, gpu_sqs_queue, working_bucket = aws_locations
        self._source_files = source_files
        ds_bucket, data_file, meta_file = source_files
        network_table, network_source = network_settings
        sample_block_size, pairs_block_size, nets_block_size = block_sizes
        data_message = {'message-type':'init-settings',
                        'data_sqs_queue': data_sqs_queue,
                        'gpu_sqs_queue': gpu_sqs_queue,
                        'working_bucket': working_bucket,
                        'ds_bucket': ds_bucket,
                        'data_file': data_file,
                        'meta_file': meta_file,
                        'network_table': network_table,
                        'network_source': network_source,
                        'sample_block_size': sample_block_size,
                        'pairs_block_size': pairs_block_size,
                        'nets_block_size': nets_block_size,
                        'gpu_mem_max': gpu_mem_max
                        }
        js_mess = json.dumps( data_message )
        self.logger.debug("DataInit message [%s]" % js_mess)
        self._send_command( js_mess )

    def send_run(self, strain, num_runs, shuffle, k):
        data_message = {'message-type':'run-instructions',
                        'strain': strain,
                        'shuffle': shuffle,
                        'k': k}
        js_mess = json.dumps( js_mess ):
        self.logger.debug("Sending run message[%s]", js_mess)
        self._send_command( js_mess )

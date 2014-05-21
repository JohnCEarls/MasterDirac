import json
import boto
import boto.utils
import boto.sqs
from boto.sqs.message import Message
import logging
from collections import deque
import serverinterface

class Interface(serverinterface.ServerInterface):
    def __init__(self, init_message):
        super( Interface, self ).__init__(init_message )
        self.logger = logging.getLogger(self._unique)
        self.logger.info("Data Interface created")
        self.num_nodes = init_message['num-nodes']
        self._active = False

    def send_init(self, aws_locations, source_files, network_settings, 
                    block_sizes, gpu_mem_max):
        self._aws_locations = aws_locations
        data_sqs_queue, data_sqs_queue_truth, gpu_sqs_queue, working_bucket = aws_locations
        self._source_files = source_files
        ds_bucket, data_file, meta_file = source_files
        network_table, network_source = network_settings
        sample_block_size, pairs_block_size, nets_block_size = block_sizes
        data_message = {'message-type':'init-settings',
                        'data_sqs_queue': data_sqs_queue,
                        'data_sqs_queue_truth': data_sqs_queue_truth,
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

    def send_run(self, run_id, strain, num_runs, shuffle, k):
        data_message = {'message-type':'run-instructions',
                        'strain': strain,
                        'shuffle': shuffle,
                        'k': k,
                        'num-runs': num_runs,
                        'run-id': run_id
                        }
        js_mess = json.dumps( data_message )
        self.logger.debug("Sending run message[%s]", js_mess)
        self._send_command( js_mess )
        self._active = True

    def _check_complete(self):
        while len(self.status_queue) > 0:
            message = self.status_queue.popleft()
            self._handle_response( message )

    def _handle_response(self, message ):
        if message['message-type'] == 'run-complete':
            self.logger.debug("Response: %s" % json.dumps(message))
            self._active = False
        else:
            self.logger.error("Error[Unexpected Response] : %s" %\
                    json.dumps( message ))
            Exception("Unexpected Response")

    @property
    def terminated(self):
        return self._terminated

    def busy(self):
        """
        Is it currently working in a run
        """
        self._check_complete()
        return self._active


    def _restart(self):
        self.logger.warning("Restart unimplemented in data node")

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
        self.logger.info( "GPU Interface created")
        self._num_gpus = None
        self.status_queue = dequeue()

    def send_init(self, aws_locations, block_sizes, data_settings, 
            heartbeat_interval=10):
        """
        Send initialization information
        aws_locations - tuple containing sqs queues and s3 buckets
                        (source_sqs, result_sqs, source_s3, result_s3)
        block_sizes - tuple containing the block sizes for gpu kernel
                    (sample_block_size, pairs_block_size, nets_block_size)
                    Note: This determines the shape of our buffered matrices
        data_settings - dictionary describing the maximum sizes and data
                        types for numpy matrices 
                        {
                         'source':[ ('mat_map_id', size, type index), ...]
                         'results':[('rms', size, type index)]
                        }
                        mat_map_id - in ['em','sm','gm','nm']
                        size - an int of max num bytes of this matrix
                        type index - is the result of 
                                masterdirac.utils.dtypes.to_index(np.dtype)
        heartbeat_interval - how many runs before checking back home
        """
        self._aws_locations = aws_locations
        source_sqs, result_sqs, source_s3, result_s3 = aws_locations
        self._block_sizes = block_sizes
        sample_block_size, pairs_block_size, nets_block_size = block_sizes
        self._data_settings = data_settings
        self._hb = heartbeat_interval
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

    @property
    def num_gpus(self):
        """
        Returns the number of gpus in this instance
        """
        if self._num_gpus is None:
            if self.instance.instance_type == 'cg1.4xlarge':
                self._num_gpus = 2
            else:
                self._num_gpus = 1
        return self._num_gpus

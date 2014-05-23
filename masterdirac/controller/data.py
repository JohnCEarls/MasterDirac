import json
import boto
import boto.utils
import boto.sqs
from boto.sqs.message import Message
import logging
from collections import deque
import serverinterface
import masterdirac.models.server as svr_mdl
import masterdirac.models.run as run_mdl

class Interface(serverinterface.ServerInterface):
    def __init__(self, init_message, master_name):
        super( Interface, self ).__init__(init_message, master_name )
        self.logger = logging.getLogger(self.unique_id)
        self.logger.info("Data Interface created")
        self.num_nodes = init_message['num-nodes']
        self._active = False
        svr_mdl.insert_ANServer( self.cluster_name, self.server_id, svr_mdl.INIT)

    def handle_state(self):
        self.get_responses()
        self.check_response()
        state = self.status
        self.logger.debug("handle_state[%s]" % state)
        if state == svr_mdl.INIT:
            self.send_init()
        elif state == svr_mdl.WAITING:
            if run_mdl.get_ANRun( self._run_id )['status'] != run_mdl.ACTIVE:
                self._run_id = None
                self.restart()
        elif state == svr_mdl.TERMINATED:
            self.delete_queues()

    def send_init(self):
        self.logger.debug("Attempting to send init")
        active_run = self._get_active_run()
        if active_run is None:
            self.logger.debug("No active runs")
            return
        self._run_id = active_run['run_id']
        intercomm_settings = active_run['intercomm_settings']
        aws_locations = (
                intercomm_settings[ 'sqs_from_data_to_agg' ],
                intercomm_settings[ 'sqs_from_data_to_agg_truth'],
                intercomm_settings[ 'sqs_from_data_to_gpu' ],
                intercomm_settings[ 's3_from_data_to_gpu'] )
        dest_data = active_run['dest_data']
        source_files = (
                dest_data[ 'working_bucket' ],
                dest_data[ 'dataframe_file' ],
                dest_data[ 'meta_file' ] )

        network_config = active_run['network_config']
        network_settings = (
                network_config[ 'network_table' ],
                network_config[ 'network_source']
                )

        run_settings = active_run['run_settings']
        block_sizes = (
                run_settings[ 'sample_block_size'],
                run_settings[ 'pairs_block_size'],
                run_settings[ 'nets_block_size'] )

        gpu_mem_max = 2*1024*1024*1024

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
        self.set_status(svr_mdl.WAITING)
    
    @property
    def server_id(self):
        return "data"

    @property
    def unique_id(self):
        return self.cluster_name

    def send_run(self, run_id, strain, num_runs, shuffle, k):
        data_message = {'message-type':'run-instructions',
                        'strain': strain,
                        'shuffle': shuffle,
                        'k': k,
                        'num-runs': num_runs,
                        'run-id': run_id
                        }
        if run_id == self._run_id:
            js_mess = json.dumps( data_message )
            self.logger.debug("Sending run message[%s]", js_mess)
            self._send_command( js_mess )
            self.set_status( svr_mdl.RUNNING )
            return True
        else:
            self.logger.warning("Rec'd a send run for a run that is not initialized")
            return False

    def restart( self ):
        data_message = {'message-type':'restart-notice'}
        js_mess = json.dumps( data_message )
        self.logger.debug("Sending run message[%s]", js_mess)
        self._send_command( js_mess )
        self.set_status( svr_mdl.RESTARTING )


    def check_response(self):
        while len(self.status_queue) > 0:
            message = self.status_queue.popleft()
            self._handle_response( message )

    def _handle_response(self, message ):
        if message['message-type'] == 'run-complete':
            self.logger.debug("Response: %s" % json.dumps(message))
            self.set_status( svr_mdl.WAITING )
        elif message['message-type'] == 'terminated':
            self.logger.info("%s terminated" % self.unique_id)
            self.set_status(svr_mdl.TERMINATED)
        elif message['message-type'] == 'restarting':
            self.logger.info("%s restarting" % self.unique_id)
            self.set_status(svr_mdl.INIT)
        else:
            self.logger.error("Error[Unexpected Response] : %s" %\
                    json.dumps( message ))
            Exception("Unexpected Response")

    def _get_active_run( self ):
        for run in run_mdl.get_ANRun():
            if run['master_name'] == self._master_name:
                if run['status'] == run_mdl.ACTIVE:
                    return run
        return None


    def busy(self):
        """
        Is it currently working in a run
        """
        self.check_response()
        return self.status != svr_mdl.WAITING

    def _restart(self):
        self.logger.warning("Restart unimplemented in data node")

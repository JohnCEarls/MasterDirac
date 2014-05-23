import json
import boto
import boto.utils
import boto.sqs
from boto.sqs.message import Message
import logging
from collections import deque
import serverinterface
import masterdirac.models.run as run_mdl
import numpy as np
from masterdirac.utils import hddata_process, dtypes
import masterdirac.models.server as svr_mdl

class Interface(serverinterface.ServerInterface):
    def __init__(self, init_message, master_name):
        super( Interface, self ).__init__(init_message, master_name )
        self._gpu_id = init_message['gpu-id']
        self.logger = logging.getLogger(self.unique_id)
        self.logger.info( "GPU Interface created")
        self._num_gpus = None
        self._idle = 0
        self._restarting = False
        svr_mdl.insert_ANServer( self.cluster_name, self.server_id, svr_mdl.INIT)
        self._waiting = 0

    def handle_state(self):
        self.get_responses()
        self.handle_heartbeat()
        state = self.status
        self.logger.debug("state[%i]" % state)
        if state == svr_mdl.INIT:
            self.send_init()
        if state == svr_mdl.WAITING:
            self.send_init()
        if state == svr_mdl.TERMINATED:
            self.delete_queues()

    def send_init( self ):
        """
        Send initialization information
        Pertinant info pulled from run model in
        _get_gpu_run. information includes:

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
        active_run = self._get_gpu_run()
        if not active_run:
            #nothing to do
            if self.status != svr_mdl.WAITING:
                self.set_status( svr_mdl.WAITING )
            return None
        self.set_status( svr_mdl.STARTING )
        self.logger.debug("Have a run %s" % active_run['run_id'])
        self._run_id = active_run['run_id']
        ic = active_run['intercomm_settings']
        self._run_id = active_run['run_id']
        source_sqs = ic['sqs_from_data_to_gpu']
        result_sqs = ic['sqs_from_gpu_to_agg']
        source_s3 = ic['s3_from_data_to_gpu']
        result_s3 = ic['s3_from_gpu_to_agg']
        #not sure this is necessary anymore
        self._aws_locations = (source_sqs, result_sqs, source_s3, result_s3)

        rs = active_run['run_settings']
        sample_block_size = rs['sample_block_size']
        pairs_block_size  = rs['pairs_block_size']
        nets_block_size   = rs['nets_block_size']

        heartbeat_interval = rs['heartbeat_interval']


        self._block_sizes = (sample_block_size, pairs_block_size, nets_block_size)

        self._data_settings = self._get_data_settings( active_run ) 
        data_settings = self._data_settings

        self._hb = heartbeat_interval
        self.logger.info("Initializing gpudirac-server" )
        gpu_message = {'message-type':'init-settings',
                 'run-id' : active_run['run_id'], 
                 'result-sqs': result_sqs,
                 'source-sqs': source_sqs,
                 'source-s3': source_s3,
                 'result-s3': result_s3,
                 'data-settings' : data_settings,
                 'gpu-id': 0, #note this is deprecated
                 'sample-block-size': sample_block_size,
                 'nets-block-size': nets_block_size,
                 'pairs-block-size': pairs_block_size,
                 'heartbeat-interval': heartbeat_interval
                }
        js_mess = json.dumps( gpu_message )
        self.logger.debug("GPUInit message [%s]" % js_mess )
        return self._send_command( js_mess )

    @property
    def idle(self):
        return self._idle

    @property
    def gpu_id(self):
        return self._gpu_id

    @property
    def server_id(self):
        return "gpu-%i" % self.gpu_id

    @property
    def unique_id(self):
        return "%s-%s" %( self.cluster_name, self.server_id )

    def _get_data_settings(self, run): 
        """
        Get an upper bound on the size of data structure s
        Returns max_bytes for  em, sm, gm, nm, rms, total
        TODO:make this more accurate
        """
        def rup( init, cut):
            return init + (cut - init%cut)
        data_sizes = run['data_sizes']  
        max_nsamples, max_ngenes, max_nnets, max_comp= data_sizes
        rs = run['run_settings']
        sample_block_size = rs['sample_block_size']
        pairs_block_size  = rs['pairs_block_size']
        nets_block_size   = rs['nets_block_size']

        k = int(rs['k'])

        b_samp = rup(max_nsamples, sample_block_size)
        b_comp = rup(max_comp, pairs_block_size)
        b_nets = rup(max_nnets+1, nets_block_size)
        em = b_samp*max_ngenes*np.float32(1).nbytes
        sm = b_samp*k*np.int32(1).nbytes
        gm = 2*b_comp*np.int32(1).nbytes
        nm = b_nets*np.int32(1).nbytes
        rt = b_comp*b_samp*np.int32(1).nbytes
        srt = rt
        rms = b_nets*b_samp*np.int32(1).nbytes
        self._data_settings = {
                'source': [
                ('em', em, dtypes.to_index(np.float32) ),
                ('gm', gm, dtypes.to_index(np.int32) ),
                ('sm', sm, dtypes.to_index(np.int32) ),
                ('nm', nm, dtypes.to_index(np.int32) )],
                'results':[
                ('rms', rms, dtypes.to_index(np.float32) )
                ]
                }
        self._gpu_mem_req = em+sm+gm+rt+srt+rms
        return self._data_settings

    def _get_gpu_run( self ):
        for run in run_mdl.get_ANRun():
            if run['master_name'] != self._master_name:
                #run does not belong to this master
                continue
            elif run['status'] in [ run_mdl.ACTIVE_ALL_SENT, run_mdl.ACTIVE]:
                return run
        return None

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

    def _restart( self ):
        gpu_message = {'message-type':'restart-notice'}
        js_mess = json.dumps( gpu_message )
        self._send_command( js_mess )
        self.logger.info("Restarting gpu server")
        self.set_status(svr_mdl.RESTARTING)


    @property
    def terminated( self ):
        return self.status == svr_mdl.TERMINATED

    def handle_heartbeat(self):
        while len(self.status_queue) > 0:
            mess = self.status_queue.pop()
            if 'message' in mess:
                if mess['message'] == 'terminated':
                    self.set_status( svr_mdl.TERMINATED )
                    return
            term = mess['terminating']
            if term == 0:
                if mess['source-q'] == 0:
                    if not self.status == svr_mdl.RESTARTING:
                        run = run_mdl.get_ANRun( self._run_id )
                        if run['status'] == run_mdl.COMPLETE:
                            self._idle += 1#count to 10 to see if done
                            if self._idle > 10:
                                self._run_id = None
                                self._restart()
                else:
                    self._idle = 0
            else:
                self._terminated = True

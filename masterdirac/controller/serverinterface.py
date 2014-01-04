import json
import boto
import boto.utils
import boto.sqs
from boto.sqs.message import Message
import logging
from masterdirac.utils import dtypes
import numpy as np
import collections


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

import gpu
import data
class ServerManager:
    """
    Manages and Initializes GPU and Data Servers
    """
    def __init__(self, data_sizes, config):
        self.logger = logging.getLogger("ServerManager")
        #mainly to keep track of instance variables
        #these are set in following methods
        self.block_sizes = None
        self.k = None
        self._gpu_mem_req = None
        self.data_settings = None
        self._init_q = None
        self.work = collections.deque()
        self.gpu_servers = {}
        self.data_servers = {}
        self.aws_locations = {}
        self.gpu_ids = {}
        #now we actually set all of these variables
        self.logger.debug("Loading Configs")
        self._load_config(config)
        self._configure_data(config)
        self._configure_gpu(config, data_sizes)
        self.logger.debug("exit __init__")

    def add_work( self, work):
        for job in work:
            self.work.appendleft(job)


    def has_work(self):
        return len(self.work) > 0

    def poll_for_server(self, timeout=20):
        messages = self.init_q.get_messages( wait_time_seconds = timeout )
        for mess in messages:
            serv_mess = json.loads( mess.get_body() )
            self.init_q.delete_message( mess )
            self._handle_server_init( serv_mess )

    def send_run( self ):
        """
        Sends chunks of work from the work queue
        to available data clusters
        """
        chunksize = 100
        if len(self.data_servers):
            for k,server in self.data_servers.iteritems():
                if len(self.work):
                    current_job = self.work.pop()
                    strain, total_runs, shuffle, k = current_job
                    num_runs = min( chunksize, total_runs)
                    total_runs -= num_runs
                    if total_runs > 0:
                        self.work.append( ( strain, total_runs, shuffle, k ) )
                    server.send_run( strain, num_runs, shuffle, k )
                else:
                    self.logger.info("No jobs")
        else:
            self.logger.warning("No data servers")

    def inspect():
        """
        TODO: load balancing, check for problems, etc
        """
        pass

    def _handle_server_init( self, message ):
        if message['message-type'] == 'gpu-init':
            self._handle_gpu_init( message )
        if message['message-type'] == 'data-init':
            self._handle_data_init( message )

    def _handle_gpu_init( self, message):
        serv = gpu.Interface( message )
        gpu_id = 0
        if serv.num_gpus > 1:
            inst_id = serv.instance_id
            if inst_id in self.gpu_ids:
                if len(self.gpu_ids[inst_id]) >= serv.num_gpus:
                    #we have too many gpus started
                    raise Exception("need to add logic for handling"
                            "gpu contention.")
                else:
                    if self.gpu_ids[inst_id][0] == 0:
                        gpu_id = 1
                    self.gpu_ids[inst_id].append(gpu_id)
            else:
                self.gpu_ids[inst_id] = [gpu_id]
        serv.send_init( self.aws_locations['gpu'], self.block_sizes,
                self.data_settings, gpu_id=gpu_id,
                heartbeat_interval=self.base_heartbeat )
        self.gpu_servers[serv._unique] = serv

    def _handle_data_init( self, message):
        serv = data.Interface( message )
        serv.send_init( self.aws_locations['data'], self.source_files, 
                self.network_settings, self.block_sizes, self.get_mem_max() )
        self.data_servers[serv._unique] = serv

    def get_mem_max(self):
        """
        Return the maximum memory of a gpu,
        This should be handled by master server, and I need to 
        remove the functionality from data servers ... but not today
        """
        return 2*1024*1024*1024

    @property
    def init_q(self):
        """
        Returns the Server Initialization Queue
        """
        conn = boto.sqs.connect_to_region('us-east-1')
        iq = conn.get_queue( self._init_q )
        while not iq:
            time.sleep(1)
            self.logger.warning("Creating Server Initialization Queue")
            iq = conn.create_queue( self.init_q )
        return iq


    def _load_config(self, config ):
        """
        Loads run specific settings from config file
        """
        self._init_q = config.get( 'run_settings','server_initialization_queue') 
        self.block_sizes = (
                config.getint('run_settings', 'sample_block_size'),
                config.getint('run_settings', 'pairs_block_size'),
                config.getint('run_settings', 'nets_block_size') )
        self.k = config.getint('run_settings', 'k')
        self.base_heartbeat = config.getint('run_settings','heartbeat_interval')

    def _configure_data(self, config ):
        self.logger.info( "Configuring data server settings" )
        self._configure_data_aws_locations( config )
        self._configure_data_source_files( config )
        self._configure_data_network_settings( config )

    def _configure_gpu( self, config, data_sizes):
        self.logger.info( "Configuring gpu server settings" )
        self._configure_gpu_aws_locations( config )
        self._configure_gpu_data_settings( data_sizes )

    def _configure_data_aws_locations(self, config):
        self.aws_locations['data'] = (
                config.get( 'intercomm_settings', 'sqs_from_data_to_agg' ),
                config.get( 'intercomm_settings', 'sqs_from_data_to_gpu' ),
                config.get( 'intercomm_settings', 's3_from_data_to_gpu') )

    def _configure_data_source_files( self, config):
        self.source_files = ( 
                config.get( 'dest_data', 'working_bucket' ),
                config.get( 'dest_data', 'dataframe_file' ),
                config.get( 'dest_data', 'meta_file' ) )

    def _configure_data_network_settings( self, config ):
        self.network_settings = (
                config.get( 'network_config', 'network_table' ),
                config.get( 'network_config', 'network_source')
                )


    def _configure_gpu_aws_locations( self, config):
        self.aws_locations['gpu'] = ( 
                config.get( 'intercomm_settings', 'sqs_from_data_to_gpu'),
                config.get( 'intercomm_settings', 'sqs_from_gpu_to_agg'),
                config.get( 'intercomm_settings', 's3_from_data_to_gpu'),
                config.get( 'intercomm_settings', 's3_from_gpu_to_agg') )

    def _configure_gpu_data_settings(self, data_sizes):
        """
        Creates the data settings for gpu memory allocation
        """
        self.logger.info("Configuring run")
        self._set_data_ub(self.k, data_sizes, self.block_sizes)

    def _set_data_ub(self, k, data_sizes, block_sizes ):
        """
        Get an upper bound on the size of data structures
        Returns max_bytes for  em, sm, gm, nm, rms, total
        TODO:make this more accurate
        """
        def rup( init, cut):
            return init + (cut - init%cut)
        max_nsamples, max_ngenes, max_nnets, max_comp= data_sizes
        sample_block_size, pairs_block_size, net_block_size = block_sizes
        b_samp = rup(max_nsamples, sample_block_size)
        b_comp = rup(max_comp, pairs_block_size)
        b_nets = rup(max_nnets+1, net_block_size)
        em = b_samp*max_ngenes*np.float32(1).nbytes
        sm = b_samp*k*np.int32(1).nbytes
        gm = 2*b_comp*np.int32(1).nbytes
        nm = b_nets*np.int32(1).nbytes
        rt = b_comp*b_samp*np.int32(1).nbytes
        srt = rt
        rms = b_nets*b_samp*np.int32(1).nbytes
        self.data_settings = {
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
if __name__ == "__main__":
    si = ServerInterface({'name':'test', 'command':'test', 
        'response':'test', 'instance-id':'test', 'zone':'test'})


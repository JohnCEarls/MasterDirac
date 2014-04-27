import json
import boto
import boto.utils
import boto.sqs
from boto.sqs.message import Message
import logging
from masterdirac.utils import dtypes
import numpy as np
import collections
import time
import boto.ec2
from datetime import datetime
import select
import masterdirac.models.master as mstr_mdl
import masterdirac.models.worker as wkr_mdl
import masterdirac.models.systemdefaults as sys_def_mdl
import os
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
        self.status_queue = collections.deque()
        self._terminated = False

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
        self._terminated = True

import gpu
import data
class ServerManager:
    """
    Manages and Initializes GPU and Data Servers
    """
    def __init__(self, data_sizes, master_model, run_model):
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
        self.cluster_startup_processes = []
        self._master_model = master_model
        self._run_model = run_model
        self._launcher_model = sys_def_mdl.get_system_defaults( 
                setting_name = 'launcher_config', component='Master' )
        #now we actually set all of these variables
        self.logger.debug("Loading Configs")
        self._load_config()
        self._configure_data()
        self._configure_gpu(data_sizes)
        assert self._gpu_mem_req < self.get_mem_max, "Not enough memory on gpu"
        self.logger.debug("exit __init__")


    def add_work( self, work):
        for job in work:
            self.work.appendleft(job)

    def terminate_data_servers(self):
        for k,server in self.data_servers.iteritems():
            server.terminate()
            self.logger.info("%s(data server): sent termination signal" % k)

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
        chunksize = self._run_model['run_settings']['chunksize']
        if len(self.data_servers):
            for k,server in self.data_servers.iteritems():
                if len(self.work) and not server.busy():
                    current_job = self.work.pop()
                    run_id, strain, total_runs, shuffle, k = current_job
                    num_runs = min( chunksize, total_runs)
                    total_runs -= num_runs
                    if total_runs > 0:
                        self.work.append( ( run_id, strain, total_runs, shuffle, k ) )
                    server.send_run(run_id, strain, num_runs, shuffle, k )
                else:
                    self.logger.info("No jobs")
        else:
            self.logger.warning("No data servers")

    def inspect(self):
        """
        TODO: load balancing, check for problems, etc
        Currently just grabs messages and puts them in deque
        returns whether to shutdown server
        """
        for k,server in self.data_servers.iteritems():
            if server.get_responses():
                self.logger.info("%s(data server): has response" % k)
        for k,server in self.gpu_servers.iteritems():
            if server.get_responses():
                self.logger.info("%s(gpu server): has response" % k)
        self.poll_launcher()

        return False

    def _handle_server_init( self, message ):
        self.logger.debug("Handling message %s" % json.dumps( message ) )
        if message['message-type'] == 'gpu-init':
            self.logger.info("Initializing gpu")
            self._handle_gpu_init( message )
        elif message['message-type'] == 'data-gen-init':
            self.logger.info("Initializing Data")
            self._handle_data_init( message )
        else:
            self.logger.error("Init. Message: %s" %  json.dumps( message ) )
            raise Exception("No initialization found in initialization message")

    def launch_cluster(self, worker_id ):
        """
        Given a worker id, prepare environment and start cluster.
        """
        worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
        if worker_model.status != wkr_mdl.CONFIG:
            self.logger.error(( 'Attempted to startup [%s]' 
                ' and status is wrong [%r]') % (worker_id, worker_model))
            raise Exception(('Attempted to startup [%s] and' 
                ' it is not in a CONFIG stat') % (worker_id) )
        key_name, key_path = self._prep_launch_region( 
                worker_model['aws_region'] )
        worker_model['starcluster_config']['key_name'] = key_name
        worker_model['starcluster_config']['key_location'] = key_path
        self.logger.info("Updating worker[%s] key information" % worker_id)
        wkr_mdl.update_ANWorker( worker_id,
                starcluster_config = worker_model['starcluster_config'] )
        startup_process = multiprocessing.Process( target = run_sc, 
                args=( worker_id, ), 
                name=worker_id)
        startup_process.start()
        self.logger.info( 'started startup process for [%s]' % (worker_id,) )
        self.cluster_startup_processes.append( startup_process )

    def poll_launcher( self ):
        messages = self.launcher_q_in.get_messages( wait_time_seconds = timeout )
        for mess in messages:
            launch_mess = json.loads( mess.get_body() )
            self.init_q.delete_message( mess )
            self._handle_launcher( launch_mess )

    def _handle_launcher( self, launch_mess):
        if launch_mess['
        #TODO: handle launch message
 
    def _prep_launch_region( self, region ):
        """
        Returns ( key_name, key_path )
        """
        if region in self._master_model['key_pairs']:
            key_name = self._master_model['key_pairs'][region]
            sys_d = self._launcher_model
            key_location = os.path.join( sys_d['key_location'], 
                    self._get_key_file_name( key_name ) )
            if os.path.exists( key_location ):
                return ( key_name, key_location )
            else:
                E_MESS = "Key [%s] is not at [%s]" % (
                    key_name, key_location )
                self.logger.error( E_MESS )
                raise Exception( E_MESS )
                #this is unrecoverable because we have a key
                #registered that may be being used,
                #but can't find it.
        else:
            return self._gen_key( region )



    def _gen_key( self, aws_region ):
        ec2 = boto.ec2.connect_to_region( aws_region )
        k_file = self._get_key_file_name( key_name )
        sys_d = self._launcher_model
        key_path = os.path.join( sys_d['key_location'], k_file )
        if os.path.isfile( key_path ) and ec2.get_key_pair( key_name ):
            #we have a key and key_pair
            return ( key_name, key_path )
        elif os.path.isfile( key_path ):
            #we have a key and no key pair
            os.remove( key_path )
        elif ec2.get_key_pair( key_name ):
            #we have a key_pair, but no key
            ec2.delete_key_pair( key_name )
        self.logger.info('Generating key [%s] at [%s] for [%s]' % (
           key_name, key_path, aws_region ) )
        key = ec2.create_key_pair( key_name )
        key.save( sys_d['key_location'] )
        os.chmod( key_path, 0600 )
        self._master_model['key_pairs'][aws_region] = key_name
        self.logger.info("Updating master model")
        mstr_mdl.insert_master( self._master_model['master_name'], 
                key_pairs = self._model['key_pairs'])
        
        return ( key_name, key_path )

    def _gen_key_name( self, aws_region):
        """
        Given region, generate a key name specific to this master
        """
        key_name = 'sc-key-%s-%s' % (self._master_model['master_name'], aws_region )
        return key_name

    def _get_key_file_name( self, key_name):
        """
        Given key name, return key file name
        """
        #just a centralized place to pull this
        return "%s.pem" % key_name


    def _handle_gpu_init( self, message):
        """
        Manage gpu server startup.
        """
        serv = gpu.Interface( message )
        gpu_id = 0
        if serv.num_gpus > 1:
            inst_id = serv.instance_id
            if inst_id in self.gpu_ids:
                self.logger.warning(("Adding gpu server to already running gpu"
                    " instance-id[%s], current config %r") %
                    (inst_id, self.gpu_ids[inst_id]) )
                #for now we just swap LRU
                #DEBUG: need better logic here
                self.logger.debug("***Revisit multigpu logic***")
                if self.gpu_ids[inst_id][-1] == 0:
                    gpu_id = 1
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
        iq = conn.create_queue( self._init_q )
        ctr = 0
        while not iq:
            time.sleep(max(ctr,60))
            self.logger.warning("Creating Server Initialization Queue")
            iq = conn.create_queue( self._init_q )
            ctr = 1.25 *ctr + 1
        return iq

    @property
    def launcher_q_in(self):
        conn = boto.sqs.connect_to_region('us-east-1')
        lq = conn.create_queue( self._launcher_model['launcher_sqs_in'] )
        ctr = 0
        while not lq:
            time.sleep(max(ctr,60))
            self.logger.warning("Creating Server Initialization Queue")
            lq = conn.create_queue(self._launcher_model['launcher_sqs_in'])
            ctr = 1.25 *ctr + 1
        return lq


    @property
    def launcher_q_out(self):
        conn = boto.sqs.connect_to_region('us-east-1')
        lq = conn.create_queue( self._launcher_model['launcher_sqs_out'] )
        ctr = 0
        while not lq:
            time.sleep(max(ctr,60))
            self.logger.warning("Creating Server Initialization Queue")
            lq = conn.create_queue(self._launcher_model['launcher_sqs_out'])
            ctr = 1.25 *ctr + 1
        return lq

    def _load_config(self ):
        """
        Loads run specific settings from config file
        """
        run_settings = self._run_model['run_settings']
        self._init_q = run_settings['server_initialization_queue']
        self.block_sizes = (
                run_settings[ 'sample_block_size'],
                run_settings[ 'pairs_block_size'],
                run_settings[ 'nets_block_size'] )
        self.k = run_settings[ 'k']
        self.base_heartbeat = run_settings['heartbeat_interval']

    def _configure_data(self):
        self.logger.info( "Configuring data server settings" )
        self._configure_data_aws_locations()
        self._configure_data_source_files()
        self._configure_data_network_settings()

    def _configure_gpu( self, data_sizes):
        self.logger.info( "Configuring gpu server settings" )
        self._configure_gpu_aws_locations( )
        self._configure_gpu_data_settings( data_sizes )

    def _configure_data_aws_locations(self):
        intercomm_settings = self._run_model['intercomm_settings']
        self.aws_locations['data'] = (
                intercomm_settings[ 'sqs_from_data_to_agg' ],
                intercomm_settings[ 'sqs_from_data_to_agg_truth'],
                intercomm_settings[ 'sqs_from_data_to_gpu' ],
                intercomm_settings[ 's3_from_data_to_gpu'] )

    def _configure_data_source_files( self ):
        dest_data = self._run_model['dest_data']
        self.source_files = (
                dest_data[ 'working_bucket' ],
                dest_data[ 'dataframe_file' ],
                dest_data[ 'meta_file' ] )

    def _configure_data_network_settings( self ):
        network_config = self._run_model['network_config']
        self.network_settings = (
                network_config[ 'network_table' ],
                network_config[ 'network_source']
                )


    def _configure_gpu_aws_locations( self ):
        intercomm_settings = self._run_model['intercomm_settings']
        self.aws_locations['gpu'] = (
                intercomm_settings[ 'sqs_from_data_to_gpu' ],
                intercomm_settings[ 'sqs_from_gpu_to_agg' ],
                intercomm_settings[ 's3_from_data_to_gpu' ],
                intercomm_settings[ 's3_from_gpu_to_agg' ] )

    def _configure_gpu_data_settings(self, data_sizes):
        """
        Creates the data settings for gpu memory allocation
        """
        self.logger.info("Configuring run")
        self._set_data_ub(self.k, data_sizes, self.block_sizes)

    def _set_data_ub(self, k, data_sizes, block_sizes ):
        """
        Get an upper bound on the size of data structure s
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

############################################
# Shared Worker Cluster Server startup
# functions
############################################


def log_subprocess_messages( sc_p, q, base_message):
    """
    Reads messages from stdout/stderr and writes them to
    given queue(q).

    Used on cluster startup to observe progress

    sc_p : subprocess that is launching the cluster
    q : boto.sqs.Queue that accepts messages
    base_message : dictionary that contains the message template
    """
    stdout = []
    stderr = []
    errors = False
    ctr = 0
    while True:
        reads = [sc_p.stdout.fileno(), sc_p.stderr.fileno()]
        ret = select.select(reads, [], [])
        for fd in ret[0]:
            ctr += 1
            base_message['time'] = datetime.now().isoformat()
            base_message['count'] = ctr
            if fd == sc_p.stdout.fileno():
                read = sc_p.stdout.readline()
                base_message['type'] ='stdout'
                base_message['time'] = datetime.now().isoformat()
                base_message['msg'] = read.strip()
                q.write(Message(body=json.dumps(base_message)))
            if fd == sc_p.stderr.fileno():
                read = sc_p.stderr.readline()
                base_message['type'] ='stderr'
                base_message['msg'] = read.strip()
                q.write(Message(body=json.dumps(base_message)))
        if sc_p.poll() != None:
            #process exitted
            ctr += 1
            base_message['time'] = datetime.now().isoformat()
            base_message['count'] = ctr
            base_message['type'] = 'system'
            base_message['msg'] = 'Complete'
            if errors:
                base_message['msg'] += ':Errors exist'
            q.write(Message(body=json.dumps(base_message)))
            break

def run_sc( worker_id ):
    """
    This runs the starcluster commands necessary to instantiate
    a worker cluster.
    """
    launcher_config = sd_model.get_system_defaults(
            setting_name='launcher_config', component='Master')

    #path to starcluster exe
    starcluster_bin = launcher_config['starcluster_bin']
    #url to page that returns cluster config template
    base_sc_config_url = launcher_config['sc_config_url']
    sc_config_url = base_sc_config_url + '/' + worker_id
    worker_model = get_ANWorker( worker_id=worker_id )
    master_name = worker_model['master_name']
    cluster_name = worker_model['cluster_name']
    pid = multiprocessing.current_process()
    wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.STARTING,
            startup_pid=pid)

    base_message = {'cluster_name': cluster_name, 'master_name': master_name, 'pid':str(pid) }
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue(launcher_config['startup_logging_queue'])

    sc_command = "%s -c %s/%s/%s start -c %s %s" %( os.path.expanduser(starcluster_bin), url,master_name, cluster_name, cluster_name, cluster_name)
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    adv_ser.set_active()
    adv_ser.set_startup_pid(str(sc_p.pid))
    log_subprocess_messages( sc_p, q, base_message)

if __name__ == "__main__":
    si = ServerInterface({'name':'test', 'command':'test',
        'response':'test', 'instance-id':'test', 'zone':'test'})


import json
import logging
import collections
import time
from datetime import datetime, timedelta
import select
import os
import os.path
import base64
import multiprocessing
import subprocess
import cgi

import numpy as np

import boto
import boto.utils
import boto.sqs
from boto.sqs.message import Message
import boto.ec2
import boto.dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.items import Item
from boto.s3.key import Key
from boto.exception import S3ResponseError

import datadirac.data
import datadirac.aggregator.controller
import masterdirac.models.master as master_mdl
import masterdirac.models.worker as wkr_mdl
import masterdirac.models.systemdefaults as sys_def_mdl
import masterdirac.models.run as run_mdl
from masterdirac.utils import hddata_process, dtypes
import gpu
import data

class ServerManager:
    """
    Manages and Initializes GPU and Data Servers
    """
    def __init__(self, master_name):
        self.logger = logging.getLogger("ServerManager")
        #mainly to keep track of instance variables
        #these are set in _configure_run
        self.logger.debug("ServerManager.__init__( %s )" % (master_name) )
        self.block_sizes = None
        self.k = None
        self._gpu_mem_req = None
        self.data_settings = None
        self._init_q = None
        self.work = collections.deque()
        self._rc = collections.defaultdict(int)
        self._complete_timeout = None
        self.gpu_servers = {}
        self.data_servers = {}
        self.aws_locations = {}
        self.gpu_ids = {}
        self._update_worker_state = False 
        self.cluster_startup_processes = []
        self.cluster_termination_processes = []
        self._master_name = master_name
        self._run_model = None
        self._local_settings = self._get_local_settings()
        self._launcher_model = self._get_launcher_config()
        self._init_queues()
        self._master_model = self._get_master_model()
        self.logger.debug("exit __init__")

    def manage_run( self ):
        logger = self.logger
        active_run = self.get_run()
        if active_run and self.status != master_mdl.RUN:
            self.status = master_mdl.RUN
        if active_run is not None:
            try:
                self._configure_run( active_run )
            except:
                self.logger.exception("Attempt to configure [%r] failed" % (
                    active_run))
                self.status = master_mdl.INIT
                return
            self.logger.debug("In manage run pre add work")
            self.logger.debug("self.has_work == [%r]" % self.has_work() )
            if not self.has_work():
                self.logger.info("Adding work")
                self._add_work( self.get_work( perm=False ) )
                self._add_work( self.get_work( perm=True ) )
        if self.has_work():
            self.send_run()
            if not self.has_work():
                self._run_model = run_mdl.update_ANRun(self._run_model['run_id'],
                        status = run_mdl.ACTIVE_ALL_SENT )
                self.status = master_mdl.INIT
                run_mdl.compress_checkpoint( self._run_model['run_id'] )
        logger.debug("Exiting manage_run")

    def poll_launch_requests( self, timeout=20 ):
        """
        Checks launcher_q_in for messages from web server
        Stuff like start cluster, etc.
        """
        while self.launcher_q_in.count() > 0:
            messages = self.launcher_q_in.get_messages( wait_time_seconds = timeout )
            for mess in messages:
                launch_mess = json.loads( mess.get_body() )
                self.launcher_q_in.delete_message( mess )
                self._handle_launcher( launch_mess )

    def poll_sc_logging( self, timeout=2):
        messages = self.sc_logging_q.get_messages(num_messages=10,
                wait_time_seconds= timeout)
        log_list = []
        while len(messages) > 0:
            for mess in messages:
                sc_logging_mess = json.loads( mess.get_body() )
                self.sc_logging_q.delete_message( mess )
                log = self._handle_sc_logging( sc_logging_mess )
                if log:
                    log_list.append( log )
            messages = self.sc_logging_q.get_messages(num_messages=10,
                wait_time_seconds= timeout)
        if len(log_list):
            with wkr_mdl.ANWorkerLog.batch_write() as batch:
                for log in log_list:
                    batch.save( wkr_mdl.ANWorkerLog( log['worker_id'],
                        log['time'], message=log['message'] ) )


    def poll_for_server(self, timeout=20):
        """
        Checks for messages indicating that a worker server has started
        and is waiting for instructions
        """
        while self.init_q.count() > 0:
            self.logger.debug("Polling for server[%s]" % self.init_q.id )
            messages = self.init_q.get_messages( wait_time_seconds = timeout )
            self.logger.debug("[%i] server init messages" % self.init_q.count() )
            for mess in messages:
                serv_mess = json.loads( mess.get_body() )
                self.logger.debug("Recieved init [%r]" % serv_mess)
                self._handle_server_init( serv_mess )
                self.init_q.delete_message( mess )

    def introspect(self):
        """
        TODO: load balancing, check for problems, etc
        Currently just grabs messages and puts them in deque
        returns whether to shutdown server
        """
        self.logger.debug("introspect")
        #a started process has returned.
        #may have a new worker state
        state_change = self.check_cluster_state()
        if state_change:
            self.logger.info("A subprocess has changed state")
        poppable = []
        for k,server in self.data_servers.iteritems():
            if state_change:
                server.refresh_status()
            server.handle_state()
            if server.terminated:
                server.delete_queues()
                poppable.append(k)
        for p in poppable:
            self.data_servers.pop( p )

        poppable = []
        for k,server in self.gpu_servers.iteritems():
            if state_change:
                server.refresh_status()
            server.handle_state()
            if server.terminated:
                self.logger.warning(" add delete quees when debugged")
                #server.delete_queues()
                poppable.append(k)
        for p in poppable:
            self.gpu_servers.pop( p )

        self.poll_sc_logging()
        self.run_completed()
        return False

    def run_completed( self ):
        for run in run_mdl.get_active_ANRun():
            if run['master_name'] != self._master_name:
                #run does not belong to this master
                continue
            elif run['status']  == run_mdl.ACTIVE_ALL_SENT:
                #the data nodes are done with this run
                conn = boto.connect_sqs()
                #branch
                try:
                    #if work q is empy and result q is not growing, 
                    #run is complete
                    #hedging our bets by making sure this 
                    #has been true for at least
                    #60 seconds + the queue timeout so messages that are pending
                    #are shown

                    work = run['intercomm_settings']['sqs_from_data_to_gpu']
                    result = run['intercomm_settings']['sqs_from_gpu_to_agg']
                    work_queue = conn.get_queue( work )
                    if work_queue is None or work_queue.count() == 0:
                        rq = conn.get_queue( result )
                        rcount = rq.count()
                        if self._rc[run['run_id']] == rcount:
                            if self._complete_timeout is None:
                                self._complete_timeout = datetime.now() + timedelta( minutes= 1, seconds=rq.get_timeout() )
                            elif self._complete_timeout < datetime.now():
                                self.logger.info("Marking %s complete" % (
                                    run['run_id']))
                                run_mdl.update_ANRun( run['run_id'], 
                                                      status=run_mdl.COMPLETE )
                                self._complete_timeout = None
                        else:
                            self._complete_timeout = None
                            self._rc[run['run_id']] = rcount
                        #TODO: delete data to gpu queue
                    else:
                        self._complete_timeout = None
                        self._rc[run['run_id']] = -1 
                except:
                    self.logger.error("Failed to mark %s complete" % ( 
                        run['run_id']) )
                    self.logger.exception("Checking next run")

    def get_run( self ):
        """
        Look for active/init runs
        """
        self.logger.debug("Looking for current run")
        master_name = self._master_name
        for item in run_mdl.ANRun.scan():
            #TODO: fix Pynamo scan
            active_run = self._check_run( item )
            if active_run is not None:
                self.logger.info("Have run")
                return active_run
        self.logger.debug("No run found")
        return None

    def get_work( self, perm=True ):
        """
        Returns list of tuples of the form
        (strain, num_runs, shuffle, k)
        Going to have to modify the data size part if allowing k to float
        """
        run_model = self._run_model
        local_config = self._local_settings
        source_data = run_model['source_data']
        run_settings = run_model['run_settings']
        md = datadirac.data.MetaInfo( os.path.join(
            local_config[ 'working_dir' ],
            self._s3_strip(source_data[ 'meta_file' ]) ) )
        p = run_settings[ 'permutations' ]
        k = run_settings[ 'k' ]
        strains = md.get_strains()
        run_id = self._add_run_meta( )
        run_cp = run_mdl.get_checkpoint(run_id)
        self.logger.debug("Strains [%r]" % strains )
        if perm:
            return [(run_id, strain, p - run_cp[strain], True, k) for strain
                                in md.get_strains() if p-run_cp[strain] > 0]
        else:
            return [(run_id, strain, 1, False, k) for strain in md.get_strains() ]

    def _add_run_meta( self):
        def json_prep( dct ):
            """
            Prepares a dictionary to be jsonized
            """
            for k,v in dct.iteritems():
                try:
                    #convert datetime to string
                    dct[k] = v.isoformat()
                except AttributeError as ae:
                    pass
                    if type(v) == set:
                        dct[k] = list(v)
                    if type(v) == dict:
                        dct[k] = json_prep( v )
            return dct
        run_settings = self._run_model['run_settings']
        logger = self.logger
        logger.info('Adding metadata for this run')
        conn = boto.dynamodb2.connect_to_region( 'us-east-1' )
        run_meta_table = run_settings[ 'run_meta_table' ]
        run_id = self._run_model['run_id']
        #TODO: put this in a model
        """
        DEPRECATED:  I do not think this is used anymore,
        but I am too chicken to actually delete it
        table = Table( run_meta_table, connection = conn )
        if table.query_count( run_id__eq=run_id ) == 0:
            timestamp = datetime.utcnow().strftime('%Y.%m.%d-%H:%M:%S')
            item = Item( table, data={'run_id':run_id,  'timestamp':timestamp} )
            item['config'] = base64.b64encode( json.dumps( json_prep( self._run_model)  ) )
            #k is in config, and that will be what most queries are interested in
            #so no need to make all clients decode64 and load json
            item['k'] = run_settings[ 'k' ]
            item.save()
        """
        return run_id



    def _check_run( self, run_item ):
        """
        Checks if this run is ours for processing

        If the run is not active, makes this the owner of the run
        """
        if run_item.status == run_mdl.INIT:
            #new run
            run_item.master_name = self._master_name
            run_item.status = run_mdl.ACTIVE
            run_item.save()
            return run_mdl.to_dict(run_item)
        elif run_item.status == run_mdl.ACTIVE:
            #already active run
            if run_item.master_name == self._master_name:
                return run_mdl.to_dict(run_item)
        return None

    def _configure_run( self, run_model):
        self.logger.debug( "Configuring Run [%s]" % run_model['run_id'])
        self._run_model = run_model
        if not self.source_data_exists():
            run_mdl.update_ANRun(self._run_model['run_id'],
                    status=run_mdl.ABORT )
            self._run_model = None
            self.logger.error( "Source Data for run is not where expected")
            raise Exception("Run Aborted %r" % run_model)
        if not self.data_is_initialized() or not self.dest_data_exists():
            self.logger.info("Initializing data")
            self.init_data()
        self.logger.debug("Loading Configs")
        self._load_run_config()
        """
        self._configure_data()
        self._configure_gpu(self._run_model['data_sizes'])"""

    def data_is_initialized(self):
        """
        Grossly oversimplified
        """
        return self._run_model['data_sizes'] is not None

    def _add_work( self, work):
        for job in work:
            self.work.appendleft(job)

    def terminate_data_servers(self):
        for k,server in self.data_servers.iteritems():
            server.terminate()
            self.logger.info("%s(data server): sent termination signal" % k)

    def has_work(self):
        self.logger.debug("self.work == [%r]"  % self.work )
        return len(self.work) > 0

    def send_run( self ):
        """
        Sends chunks of work from the work queue
        to available data clusters
        """
        chunksize = self._run_model['run_settings']['chunksize']
        cp_list = []

        if len(self.data_servers):
            for k,server in self.data_servers.iteritems():
                self.logger.info("Server[%s] busy[%s]" %(k, server.busy()))
                if len(self.work) and not server.busy():
                    self.logger.info("adding work to server[%s]" % k)
                    current_job = self.work.pop()
                    run_id, strain, total_runs, shuffle, k = current_job
                    num_runs = min( chunksize, total_runs)
                    total_runs -= num_runs
                    sent = server.send_run(run_id, strain, num_runs, shuffle, k )
                    if sent:
                        if total_runs > 0:
                            self.work.append( ( run_id, strain,
                                total_runs, shuffle, k ) )
                        cp_list.append( run_mdl.pack_checkpoint(
                            run_id, num_runs, strain=strain) )
                    else:
                        self.work.append( current_job )
                        self.logger.warning("Worker[%s] rejected job")
                else:
                    self.logger.info("No jobs or all servers busy")
            if cp_list:
                run_mdl.batch_checkpoint( cp_list )
        else:
            self.logger.warning("No data servers")

    def init_data(self ):
        """
        Grabs data sources from s3, creates the dataframe needed
        by the system and moves it to another s3 bucket, along
        with metadata
        """
        self.logger.info("Getting source data")
        self._get_source_data()
        self.logger.info("Generating dataframe")
        self._generate_dataframe()
        self.logger.info("Copying metadata to working bucket")
        self._copy_metadata_file()

    def _generate_dataframe( self ):
        """
        Takes source data and generates destination data
        """
        run_model = self._run_model
        local_config = self._local_settings
        source_data = run_model['source_data']
        network_config = run_model['network_config']
        dest_data = run_model['dest_data']
        self.logger.info("Getting source data")
        self.logger.info("Generating dataframe")
        hddg = hddata_process.HDDataGen( self._local_settings['working_dir'] )
        df,net_est = hddg.generate_dataframe( source_data, network_config )
        #put datasize upper bound in run_model
        self._save_data_sizes( df, net_est )
        #writes the dataframe to the working bucket
        hddg.write_to_s3( df, dest_data )

    def _copy_metadata_file(self):
        """
        S3 to S3 copy of metadata file
        """
        source_data = self._run_model['source_data']
        dest_data = self._run_model['dest_data']
        conn = boto.connect_s3()
        source = conn.get_bucket( source_data['bucket'] )
        k = Key(source)
        k.key = source_data['meta_file']
        k.copy( dest_data[ 'working_bucket' ], dest_data['meta_file'] )
        self.logger.info("Sending metadata file to s3://%s/%s" % (
                        dest_data[ 'working_bucket' ], k.name ))

    def _s3_strip(self, key_name):
        return os.path.split(key_name)[-1]

    def _get_source_data( self):
        """
        Downloads the data from s3 to the local machine for processing
        """
        run_model = self._run_model
        local_config = self._local_settings
        if not os.path.exists(self._local_settings['working_dir']):
            self.logger.info( "Creating directory [%s]" % (
                self._local_settings['working_dir'] ) )

            os.makedirs(self._local_settings['working_dir'])
        sd = run_model['source_data']
        #grab filenames we are interested in
        file_list = [f for  k, f  in sd.iteritems() if k[-4:] == 'file']
        conn = boto.connect_s3()
        bucket = conn.get_bucket( sd['bucket'] )
        for key_name in file_list:
            s3_path, fname = os.path.split(key_name)
            local_path = os.path.join(self._local_settings['working_dir'], fname)
            try:
                self.logger.info( "Transferring s3://%s/%s to %s" % (sd['bucket'],key_name, local_path ))
                k = Key(bucket)
                k.key = key_name
                k.get_contents_to_filename(local_path)
                self.logger.info("Transfer complete")
            except S3ResponseError as sre:
                self.logger.error("bucket:[%s] file:[%s] download." % (sd['bucket'],key_name))
                self.logger.error(str(sre))
                raise(sre)
        self.logger.info("Complete")

    def _save_data_sizes( self, df, net_est):
        """
        Using the generated data frame, we save an upper bound on the
        size of the matrices that will be generated in the run_model
        """
        max_nsamples = len(df.columns)
        max_ngenes = len(df.index)
        max_nnets = len(net_est)
        max_comp = sum([x*(x-1)/2 for x in net_est])
        self.logger.debug(( "Upper Bound on matrix dims "
            "samp[%i], gen[%i],nets[%i], comp[%s]") % (
                max_nsamples, max_ngenes, max_nnets, max_comp) )
        data_sizes = (max_nsamples, max_ngenes, max_nnets, max_comp)
        self._run_model = run_mdl.update_ANRun( self._run_model['run_id'],
                    data_sizes=data_sizes )

    def source_data_exists( self ):
        """
        Check that the files we expect to be in source exist
        """
        source_data = self._run_model['source_data']
        source_keys = [
                        source_data[ 'data_file'],
                        source_data[ 'meta_file'],
                        source_data[ 'annotations_file'],
                        source_data[ 'synonym_file'],
                        source_data[ 'agilent_file']
                    ]
        conn = boto.connect_s3()
        source_bucket = conn.get_bucket( source_data['bucket'] )
        for key in source_keys:
            if not source_bucket.get_key( key , validate=True):
                return False
        return True

    def dest_data_exists( self ):
        """
        Checks if the pre-processed data( data for data nodes )
        is in the working bucket
        """
        dest_data = self._run_model[ 'dest_data' ]
        conn = boto.connect_s3()
        dest_bucket = conn.get_bucket( dest_data['working_bucket'] )
        dest_keys = [dest_data['meta_file'],
                dest_data['dataframe_file'] ]
        for key in dest_keys:
            if not dest_bucket.get_key( key , validate=True):
                return False
        return True

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
        return True

    def _handle_sc_logging(self, mess ):
        lgt=''
        if mess['type'] == 'system':
            lgt = '-success'
        if mess['type'] == 'stderr':
            lgt = '-danger'
        if mess['type'] == 'stdout':
            lgt = '-info'
        message_templ = ('<li class="list-group-item '
                        'list-group-item%s %s">'
                        '<div class="list-group-item-heading">%s</div>'
                        '<div class="action">%s</div>'
                        '<div class="message">%s</span>'
                        '</li>'
                        )
        log = {}
        if len(mess['msg'].strip()):
            log['message'] = message_templ % ( lgt, mess['type'],
                    mess['component'].upper(), mess['action'],
                    cgi.escape(mess['msg']) )
            log['worker_id'] = mess['worker_id']
            log['time'] = mess['time']
        return log

    def activate_server(self, worker_id):
        worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
        if worker_model['status'] != wkr_mdl.READY:
            self.logger.error(( 'Attempted to start server[%s]'
                ' and status is wrong [%r]') % (worker_id, worker_model))
            raise Exception(('Attempted to start server[%s] and'
                ' it is not in a READY stat') % (worker_id) )
        if worker_model['starcluster_config']['node_instance_type'] in ['cg1.4xlarge']:
            start_process = multiprocessing.Process( target = start_gpu,
                args=( worker_id, ),
                name=worker_id)
            start_process.start()
        else:
            start_process = multiprocessing.Process( target = start_data,
                args=( worker_id, ),
                name=worker_id)
            start_process.start()

    def activate_run(self, run_id):
        run_model = run_mdl.get_ANRun( run_id = run_id )
        if run_model['status'] != run_mdl.CONFIG:
            self.logger.error(( 'Attempted to start run %s]'
                ' and status is wrong [%r]') % (run_id, run_model))
            raise Exception(('Attempted to start run[%s] and'
                ' it is not in a CONFIG stat') % (run_id) )
        if self.status !=  master_mdl.INIT:
            self.logger.error(( 'Attempted to start run %s and master status'
                'is not INIT - current status[%i] ') % ( run_id, self.status ))
            raise Exception(( 'Attempted to start run %s and master status'
                'is not INIT - current status[%i] ') % ( run_id, self.status ))
        run_mdl.update_ANRun( run_id, status=run_mdl.INIT,
                master_name=self.master_model['master_name'] )


    def stop_server(self, worker_id ):
        """
        This needs to be patched to new setup
        """
        worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
        if worker_model['status'] != wkr_mdl.RUNNING:
            self.logger.error(( 'Attempted to startup [%s]'
                ' and status is wrong [%r]') % (worker_id, worker_model))
            raise Exception(('Attempted to stop server [%s] and'
                ' it is not in a RUNNING stat') % (worker_id) )
        if worker_model['starcluster_config']['node_instance_type'] in ['cg1.4xlarge']:
            stop_process = multiprocessing.Process( target = stop_gpu,
                args=( worker_id, ),
                name=worker_id)
            stop_process.start()
        else:
            stop_process = multiprocessing.Process( target = stop_data,
                args=( worker_id, ),
                name=worker_id)
            stop_process.start()

    def status_server(self,  worker_id ):
        worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
        if worker_model['starcluster_config']['node_instance_type'] in ['cg1.4xlarge']:
            status_process = multiprocessing.Process( target = status_gpu,
                args=( worker_id, ),
                name=worker_id)
            status_process.start()
        else:
            status_process = multiprocessing.Process( target = status_data,
                args=( worker_id, ),
                name=worker_id)
            status_process.start()

    def check_cluster_state(self):
        """
        See if subprocesses have finished. 
        Returns true if any subprocess has returned
        """
        new_csp = []
        update = False
        for p in self.cluster_startup_processes:
            p.join(1)
            if p.is_alive():
                new_csp.append(p)
            else:
                update = True
        self.cluster_startup_processes = new_csp
        new_ctp = []
        for p in self.cluster_termination_processes:
            p.join(1)
            if p.is_alive():
                new_ctp.append(p)
            else:
                update = True
        self.cluster_termination_processes = new_ctp
        return update


    def launch_cluster(self, worker_id ):
        """
        Given a worker id, prepare environment and start cluster.
        """
        worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
        if worker_model['status'] != wkr_mdl.CONFIG:
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

    def terminate_cluster(self, worker_id ):
        """
        Given a worker id, prepare environment and start cluster.
        """
        worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
        if wkr_mdl.confirm_worker_running( worker_model ):
            startup_process = multiprocessing.Process( target = terminate_sc,
                    args=( worker_id, ),
                    name=worker_id)
            startup_process.start()
            self.logger.info( 'started termination process for [%s]' % (worker_id,) )
            self.cluster_termination_processes.append( startup_process )
        else:
            self.logger.info('Cluster is not running, mark with error')
            wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.TERMINATED_WITH_ERROR)
            self._update_worker_state = True

    def _handle_launcher( self, launch_mess):
        """
        Handles requests from web server
        """
        self.logger.debug( "Launcher request %r" %  launch_mess )
        if launch_mess['action'] == 'activate':
            try:
                self.launch_cluster( launch_mess['worker_id'] )
            except Exception as e:
                self.logger.exception( "Attempt to launch cluster failed [%r]" % (
                    launch_mess))
                self.logger.error( "Abort launch")
                self._abort_launch( launch_mess, e )
        elif launch_mess['action'] == 'activate-server':
            try:
                self.activate_server( launch_mess['worker_id'] )
            except Exception as e:
                self.logger.exception( "Attempt to Activate server failed [%r]" % (
                    launch_mess))
                self.logger.error( "Abort activation")
                self._abort_launch( launch_mess, e )
        elif launch_mess['action'] == 'stop-server':
            try:
                self.stop_server( launch_mess['worker_id'] )
            except Exception as e:
                self.logger.exception( "Attempt to stop server failed [%r]" % (
                    launch_mess))
                self.logger.error( "Abort termination")
                self._abort_launch( launch_mess, e )
        elif launch_mess['action'] == 'status-server':
            try:
                self.status_server( launch_mess['worker_id'] )
            except Exception as e:
                self.logger.exception( "Attempt to status server failed [%r]" % (
                    launch_mess))
                self.logger.error( "Abort termination")
                self._abort_launch( launch_mess, e )
        elif launch_mess['action'] == 'terminate':
            try:
                self.terminate_cluster( launch_mess['worker_id'] )
            except Exception as e:
                self.logger.exception( "Attempt to terminate cluster failed [%r]" % (
                    launch_mess))
                self.logger.error( "Abort termination")
                self._abort_launch( launch_mess, e )
        elif launch_mess['action'] == 'activate-run':
            try:
                self.activate_run( launch_mess['run_id'] )
                mess = { 'status' : 'complete',
                        'data' : launch_mess,
                        'message': 'Activating run_id:%s' % launch_mess['run_id']}
                self.launcher_q_out.write( Message( body=json.dumps( mess ) ) )
            except Exception as e:
                self.logger.exception( "Attempt to terminate cluster failed [%r]" % (
                    launch_mess))
                self.logger.error( "Abort termination")
                self._abort_launch( launch_mess, e )
        elif launch_mess['action'] == 'reassign-master':
            try:
                run_mdl.update_ANRun( launch_mess['run_id'], master_name=this._master_name)
                mess = { 'status' : 'complete',
                        'data' : launch_mess,
                        'message': 'Reassigned master:%s' % launch_mess['run_id']}
                self.launcher_q_out.write( Message( body=json.dumps( mess ) ) )
            except Exception as e:
                self.logger.error("%r" % launch_mess)
                self.logger.exception("Unable to reassign master")
        else:
            self.logger.error("Unhandled Launcher Message")
            self.logger.error("%r" % launch_mess )
            self._abort_launch(launch_mess, "Launcher message unrecognized")

    def _abort_launch( self, launch_mess, except_obj ):
        """
        Notifies the web server that an error occurred
        """
        abort_message = {'status': 'error',
                        'data': launch_mess,
                        'message' : "%r" % (except_obj,)
                        }
        m = Message( body=json.dumps( abort_message ) )
        self.launcher_q_out.write( m )

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
        key_name = self._gen_key_name( aws_region )
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
        master_mdl.update_master( self.master_model['master_name'],
                key_pairs = self.master_model['key_pairs'])
        return ( key_name, key_path )

    def _gen_key_name( self, aws_region):
        """
        Given region, generate a key name specific to this master
        """
        key_name = 'sc-key-%s-%s' % (self._master_model['master_name'],
                aws_region )
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
        serv = gpu.Interface( message, master_name = self._master_name )
        self.gpu_servers[serv.unique_id] = serv

    def _handle_data_init( self, message ):
        serv = data.Interface( message, master_name = self._master_name)
        self.data_servers[serv.unique_id] = serv

    def get_mem_max(self):
        """
        Return the maximum memory of a gpu,
        This should be handled by master server, and I need to
        remove the functionality from data servers ... but not today
        """
        return 2*1024*1024*1024

    @property
    def status(self):
        return self.master_model['status']

    @status.setter
    def status( self, value ):
        self.master_model = {'status', value}

    @property
    def master_model(self):
        return self._master_model

    @master_model.setter
    def master_model( self, value ):
        for k,v in value.iteritems():
            self._master_model[k] = v
        self._master_model = master_mdl.update_master( **self._master_model )

    @property
    def init_q(self):
        """
        Returns the Server Initialization Queue
        """

        local_config = self._local_settings
        conn = boto.sqs.connect_to_region('us-east-1')
        iq = conn.create_queue( local_config['init-queue'] )
        ctr = 0
        while not iq:
            time.sleep(max(ctr,60))
            self.logger.warning("Creating Server Initialization Queue")
            iq = conn.create_queue( local_config['init-queue'] )
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

    @property
    def sc_logging_q(self):
        conn = boto.sqs.connect_to_region('us-east-1')
        lq = conn.create_queue( self._launcher_model['startup_logging_queue'] )
        ctr = 0
        while not lq:
            time.sleep(max(ctr,60))
            self.logger.warning("Creating Server Initialization Queue")
            lq = conn.create_queue(self._launcher_model['startup_logging_queue'])
            ctr = 1.25 *ctr + 1
        return lq

    def _init_queues(self):
        logger = self.logger
        #make sure init queue is available
        local_config = self._local_settings
        launcher_config = self._launcher_model
        conn = boto.sqs.connect_to_region( 'us-east-1' )
        q_list = [  local_config['init-queue'],
                    launcher_config['launcher_sqs_in'],
                    launcher_config['launcher_sqs_out'],
                    launcher_config['startup_logging_queue']
                    ]
        self.logger.debug("init queue names %r " % q_list )
        for q_name in q_list:
            if q_name:#cheap hack, keep getting a None queue
                q = conn.create_queue( q_name )
                if not q:
                    logger.error("%s not created" % q_name)
                    raise Exception("Unable to initialize %s" % q_name)

    def _get_master_model( self ):
        master_model = master_mdl.get_master( self._master_name )
        if not master_model:
            master_model = self._init_master_model()
        return master_model

    def _init_master_model(self):
        """
        Run if a model does not already exist
        """
        self.logger.info("Initializing Master Model")
        master_name = self._master_name
        instance_id = boto.utils.get_instance_metadata()['instance-id']
        comm_queue = self._local_settings['init-queue']
        status = master_mdl.INIT
        inst_md = boto.utils.get_instance_metadata()
        aws_region = inst_md['placement']['availability-zone'][:-1]
        return master_mdl.insert_master( master_name,
            aws_region=aws_region,
            instance_id = instance_id,
            comm_queue = comm_queue,
            status = status,
            branch = self._local_settings['branch']

            )


    def _get_local_settings(self):
        return sys_def_mdl.get_system_defaults( 'local_settings', 'Master' )

    def _get_launcher_config(self):
        return sys_def_mdl.get_system_defaults( 'launcher_config', 'Master' )

    def _load_run_config(self ):
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

    def cleanup_s3( self ):
        """
        Deletes empty buckets from old runs
        """
        s3 = boto.connect_s3()
        del_all_pattern = datadirac.aggregator.controller.DEL_LIFECYCLE_PATTERN
        for b in s3.get_all_buckets():
            try:
                config = b.get_lifecycle_config()
                for r in config:
                    if r.id == del_all_pattern % b.name:
                           self.logger.info("Deleting bucket[s3://%s]" % b.name)
                           b.delete()
            except Exception as e:
                #note: this could be thrown if bucket is not empty
                #       in which case, we'll get them after the lifecycle
                #       rule has run
                msg = "Error on attempted delete of s3://%s" % b.name
                self.logger.exception( msg )

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
    def send_msg( mtype, msg, base_message=base_message, q=q, acc = {'i':0}):
        log_message = base_message.copy()
        log_message['time'] = datetime.utcnow().isoformat()
        log_message['type'] = mtype
        log_message['msg'] = msg
        log_message['count'] = acc['i']
        q.write( Message( body=json.dumps( log_message ) ) )
        acc['i'] =acc['i'] + 1

    send_msg('system', 'Starting')
    cont = True
    reads = (sc_p.stdout, sc_p.stderr)
    while cont:
        cont = sc_p.poll() is None
        ret = select.select(reads, [], [])
        for fd in ret[0]:
            if fd.fileno() == sc_p.stdout.fileno():
                send_msg('stdout', sc_p.stdout.readline().strip() )
            if fd.fileno() == sc_p.stderr.fileno():
                send_msg('stderr', sc_p.stderr.readline().strip() )
    line = sc_p.stdout.readline().strip()
    while line != '':
        send_msg('stdout',line)
        line = sc_p.stdout.readline().strip()
    line = sc_p.stderr.readline().strip()
    while line != '':
        send_msg('stderr', line)
        line = sc_p.stderr.readline().strip()
    send_msg( 'system', 'Complete: returned[%i]' % cont )
    return cont

def run_sc( worker_id ):
    """
    This runs the starcluster commands necessary to instantiate
    a worker cluster.
    """
    launcher_config = sys_def_mdl.get_system_defaults( 'launcher_config',
            'Master' )

    #path to starcluster exe
    starcluster_bin = launcher_config['starcluster_bin']
    #url to page that returns cluster config template
    base_sc_config_url = launcher_config['sc_config_url']
    sc_config_url = base_sc_config_url + '/' + worker_id
    worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
    master_name = worker_model['master_name']
    cluster_name = worker_model['cluster_name']
    pid = str(multiprocessing.current_process().pid)
    wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.STARTING,
            startup_pid=pid)

    base_message = {
                        'worker_id' : worker_id,
                        'cluster_name': cluster_name,
                        'master_name': master_name,
                        'pid': pid,
                        'component': 'cluster-management',
                        'action' : 'start'
                    }
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue(launcher_config['startup_logging_queue'])
    sc_command = "%s -c %s start -c %s %s" % (
            os.path.expanduser(starcluster_bin),
            sc_config_url,
            cluster_name,
            cluster_name)
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    log_subprocess_messages( sc_p, q, base_message)
    ec2 = boto.ec2.connect_to_region(worker_model['aws_region'])
    instances = []
    for group in ec2.get_all_security_groups(['@sc-%s' % cluster_name ] ):
        instances += [inst.id for inst in group.instances()]
    if len(instances) > 0:
        wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.READY,
            num_nodes = len(instances), nodes= instances)
    else:
        wkr_mdl.update_ANWorker( worker_id,
                status=wkr_mdl.TERMINATED_WITH_ERROR )

def terminate_sc( worker_id ):
    launcher_config = sys_def_mdl.get_system_defaults( 'launcher_config',
            'Master' )
    #path to starcluster exe
    starcluster_bin = launcher_config['starcluster_bin']
    #url to page that returns cluster config template
    base_sc_config_url = launcher_config['sc_config_url']
    sc_config_url = base_sc_config_url + '/' + worker_id
    worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
    master_name = worker_model['master_name']
    cluster_name = worker_model['cluster_name']
    pid = str(multiprocessing.current_process().pid)
    wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.TERMINATING)
    base_message = {
                        'worker_id' : worker_id,
                        'cluster_name': cluster_name,
                        'master_name': master_name,
                        'pid': pid,
                        'component': 'cluster-management',
                        'action' : 'terminate'
                    }
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue(launcher_config['startup_logging_queue'])
    sc_command = "%s -c %s terminate -f -c %s" % (
            os.path.expanduser(starcluster_bin),
            sc_config_url,
            cluster_name)
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    log_subprocess_messages( sc_p, q, base_message)
    ec2 = boto.ec2.connect_to_region(worker_model['aws_region'])
    instances = []
    try:
        for group in ec2.get_all_security_groups(['@sc-%s' % cluster_name ] ):
            instances += [inst.id for inst in group.instances() if instance.state != "running"]
    except boto.exception.EC2ResponseError as e:
        #termination successful if no security group
        instances = []
    if len(instances) == 0:
        wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.TERMINATED)
    else:
        wkr_mdl.update_ANWorker( worker_id,
                status=wkr_mdl.TERMINATED_WITH_ERROR )
    def write_to_log( base_message, q, mtype, msg):
        log_message = base_message.copy()
        log_message['time'] = datetime.utcnow().isoformat()
        log_message['type'] = mtype
        log_message['msg'] = msg
        log_message['count'] = 99999
        q.write( Message( body=json.dumps( log_message ) ) )
    sqs = boto.sqs.connect_to_region("us-east-1")
    #remove the command and response queues created by this worker
    for q_name in worker_model['sqs_queues']:
        try:
            sqs.delete_queue( q_name )
            msg = 'Deleted queue[%s]' % q_name
            write_to_log(base_message, q, 'stdout', msg, q)
        except Exception as e:
            msg =  'Error attempting to delete queue[%s]' % q_name
            write_to_log(base_message, q, 'stderr', msg, q)

def start_gpu( worker_id ):
    """
    The entry subprocess for starting a dual gpu cluster
    """
    gpu_logserver_daemon( worker_id, action = 'start')
    gpu_daemon( worker_id, gpu_id=0, action = 'start')
    gpu_daemon( worker_id, gpu_id=1, action = 'start')
    wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.RUNNING)



def stop_gpu( worker_id ):
    """
    The entry subprocess for starting a dual gpu cluster
    """
    gpu_logserver_daemon( worker_id, action = 'stop')
    gpu_daemon( worker_id, gpu_id=0, action = 'stop')
    gpu_daemon( worker_id, gpu_id=1, action = 'stop')
    wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.READY)

def restart_gpu( worker_id ):
    gpu_daemon( worker_id, gpu_id=0, action = 'restart')
    gpu_daemon( worker_id, gpu_id=1, action = 'restart')

def status_gpu( worker_id ):
    """
    The entry subprocess for starting a dual gpu cluster
    """
    gpu_logserver_daemon( worker_id, action = 'status')
    gpu_daemon( worker_id, gpu_id=0, action = 'status')
    gpu_daemon( worker_id, gpu_id=1, action = 'status')

def start_data( worker_id ):
    data_daemon( worker_id, action="start")
    wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.RUNNING)

def status_data( worker_id ):
    data_daemon( worker_id, action="status")

def stop_data( worker_id ):
    data_daemon( worker_id, action="stop")
    wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.READY)

def restart_data( worker_id ):
    data_daemon( worker_id, action="restart")
    wkr_mdl.update_ANWorker( worker_id, status=wkr_mdl.READY)

def gpu_logserver_daemon( worker_id, action='start'):
    """
    Manages interactions with gpu logserver
    """
    launcher_config = sys_def_mdl.get_system_defaults( 'launcher_config',
            'Master' )
    #path to starcluster exe
    starcluster_bin = launcher_config['starcluster_bin']
    #url to page that returns cluster config template
    base_sc_config_url = launcher_config['sc_config_url']
    sc_config_url = base_sc_config_url + '/' + worker_id
    worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
    master_name = worker_model['master_name']
    cluster_name = worker_model['cluster_name']
    valid_actions = ['start', 'stop', 'status']
    assert action in valid_actions, "%s is not a valid action for gpu" % action
    base_message = {
                        'worker_id' : worker_id,
                        'cluster_name': cluster_name,
                        'master_name': master_name,
                        'action':action,
                        'component': 'gpu-logserver-daemon'
                    }
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue(launcher_config['startup_logging_queue'])
    sc_command = "%s -c %s sshmaster -u sgeadmin  %s " % (
            os.path.expanduser(starcluster_bin),
            sc_config_url,
            cluster_name)
    if action == 'start':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/logserver.sh start'"
    if action == 'status':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/logserver.sh status'"
    if action == 'stop':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/logserver.sh stop'"
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    log_subprocess_messages( sc_p, q, base_message)

def gpu_daemon( worker_id, gpu_id=0, action='start'):
    launcher_config = sys_def_mdl.get_system_defaults( 'launcher_config',
            'Master' )
    starcluster_bin = launcher_config['starcluster_bin']
    base_sc_config_url = launcher_config['sc_config_url']
    sc_config_url = base_sc_config_url + '/' + worker_id
    worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
    master_name = worker_model['master_name']
    cluster_name = worker_model['cluster_name']
    valid_actions = ['start', 'stop', 'status', 'restart']
    assert action in valid_actions, "%s is not a valid action for gpu" % action
    base_message = {
                        'worker_id' : worker_id,
                        'cluster_name': cluster_name,
                        'master_name': master_name,
                        'gpu_id':str(gpu_id),
                        'action':action,
                        'component':'gpuserver-daemon' }
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue(launcher_config['startup_logging_queue'])
    sc_command = "%s -c %s sshmaster -u sgeadmin  %s " % (
            os.path.expanduser(starcluster_bin),
            sc_config_url,
            cluster_name)
    if action == 'start':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/gpuserver%i.sh start'" % gpu_id
    if action == 'status':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/gpuserver%i.sh status'" % gpu_id
    if action == 'stop':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/gpuserver%i.sh stop'" % gpu_id
    if action == 'restart':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/gpuserver%i.sh restart'" % gpu_id
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    log_subprocess_messages( sc_p, q, base_message)

def data_daemon( worker_id, action="start"):
    launcher_config = sys_def_mdl.get_system_defaults( 'launcher_config',
            'Master' )
    starcluster_bin = launcher_config['starcluster_bin']
    base_sc_config_url = launcher_config['sc_config_url']
    sc_config_url = base_sc_config_url + '/' + worker_id
    worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
    master_name = worker_model['master_name']
    cluster_name = worker_model['cluster_name']
    valid_actions = ['start', 'stop', 'status', 'restart']
    assert action in valid_actions, "%s is not a valid action for data" % action
    base_message = {
                        'worker_id' : worker_id,
                        'cluster_name': cluster_name,
                        'master_name': master_name,
                        'action':action,
                        'component':'dataserver-daemon' }
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue(launcher_config['startup_logging_queue'])
    sc_command = "%s -c %s sshmaster -u sgeadmin  %s " % (
            os.path.expanduser(starcluster_bin),
            sc_config_url,
            cluster_name)
    if action == 'start':
        sc_command += "'bash /home/sgeadmin/DataDirac/scripts/datadirac.sh start'"
    if action == 'status':
        sc_command += "'bash /home/sgeadmin/DataDirac/scripts/datadirac.sh status'"
    if action == 'stop':
        sc_command += "'bash /home/sgeadmin/DataDirac/scripts/datadirac.sh stop'"
    if action == 'restart':
        sc_command += "'bash /home/sgeadmin/DataDirac/scripts/datadirac.sh restart'"
    base_message['command'] = sc_command
    print sc_command
    sc_p = subprocess.Popen( sc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    log_subprocess_messages( sc_p, q, base_message)


def cluster_restart( worker_id ):
    launcher_config = sys_def_mdl.get_system_defaults( 'launcher_config',
            'Master' )
    base_sc_config_url = launcher_config['sc_config_url']
    sc_config_url = base_sc_config_url + '/' + worker_id
    worker_model = wkr_mdl.get_ANWorker( worker_id=worker_id )
    master_name = worker_model['master_name']
    cluster_name = worker_model['cluster_name']
    base_message = {'worker_id' : worker_id,
                    'cluster_name': cluster_name,
                    'master_name': master_name,
                    'component': 'cluster-management',
                    'action' : 'restart'
                   }
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue(launcher_config['startup_logging_queue'])
    sc_command = "%s -c %s restart %s " % (
            os.path.expanduser(starcluster_bin),
            sc_config_url,
            cluster_name)
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    log_subprocess_messages( sc_p, q, base_message)


if __name__ == "__main__":
    si = ServerInterface({'name':'test', 'command':'test',
        'response':'test', 'instance-id':'test', 'zone':'test'})


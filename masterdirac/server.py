import boto.dynamodb2
from boto.s3.key import Key
from utils import hddata_process
import numpy as np
import datadirac.data
import os.path
import logging
import controller.serverinterface
import json
import base64
import random
import string
from boto.dynamodb2.table import Table
import datetime
from boto.dynamodb2.items import Item
import boto.utils
import masterdirac.models.systemdefaults as sys_def
import masterdirac.models.master as master_mdl
import time

def get_master_name():
    inst_id = boto.utils.get_instance_metadata()['instance-id']
    print inst_id
    return inst_id

def init_data( run_model ):
    """
    Grabs data sources from s3, creates the dataframe needed
    by the system and moves it to another s3 bucket
    """
    local_config = sys_def.get_system_defaults('local_settings', 'Master')
    source_data = run_model['source_data']
    dest_data = run_model['dest_data']
    network_config = run_model['network_config']

    args = ( source_data[ 'bucket'],
            source_data[ 'data_file'],
            source_data[ 'meta_file'],
            source_data[ 'annotations_file'],
            source_data[ 'synonym_file'],
            source_data[ 'agilent_file'],
            local_config['working_dir'] )
    logger = logging.getLogger('DataInit')
    logger.info("Getting source data")

    hddata_process.get_from_s3( *args )
    logger.info("Generating dataframe")
    hddg = hddata_process.HDDataGen( local_config['working_dir'] )
    df,net_est = hddg.generate_dataframe( source_data[ 'data_file'],
                                source_data[ 'annotations_file'],
                                source_data[ 'agilent_file'],
                                source_data[ 'synonym_file'],
                                network_config[ 'network_table'],
                                network_config[ 'network_source'])

    logger.info("Sending dataframe to s3://%s/%s" % ( 
        dest_data[ 'working_bucket'], 
        dest_data[ 'dataframe_file'] ) )
    hddg.write_to_s3( dest_data[ 'working_bucket'],
            df, dest_data[ 'dataframe_file'] )
    conn = boto.connect_s3()
    source = conn.get_bucket( source_data['bucket'] )
    k = Key(source)
    k.key = source_data['meta_file']
    k.copy( dest_data[ 'working_bucket' ], k.name )
    logger.info("Sending metadata file to s3://%s/%s" % (
                    dest_data[ 'working_bucket' ], k.name ))
    max_nsamples = len(df.columns)
    max_ngenes = len(df.index)
    max_nnets = len(net_est)
    max_comp = sum([x*(x-1)/2 for x in net_est])
    logger.debug( "UB on dims4 samp[%i], gen[%i],nets[%i], comp[%s]" % (
                max_nsamples, max_ngenes, max_nnets, max_comp) )
    return (max_nsamples, max_ngenes, max_nnets, max_comp)

def init_infrastructure():
    logger = logging.getLogger('init_infrastructure')
    #make sure init queue is available
    local_config =  sys_def.get_system_defaults( 'local_settings', 'Master' )
    launcher_config =  sys_def.get_system_defaults( 'launcher_config', 'Master' )
    conn = boto.sqs.connect_to_region( 'us-east-1' )
    q = conn.create_queue( local_config['init-queue'] )
    if not q:
        logger.error("Init Q not created")
        raise Exception("Unable to initialize Init Q")
    q = conn.create_queue( launcher_config['launcher_sqs_in'] )
    if not q:
        logger.error("launcher in Q not created")
        raise Exception("Unable to initialize launcher in Q")
    q = conn.create_queue( launcher_config['launcher_sqs_out'] )
    if not q:
        logger.error("launcher out Q not created")
        raise Exception("Unable to initialize launcher out Q")
    master_model = master_mdl.get_master( get_master_name() )
    if not master_model:
        master_model = init_master_model()
    return master_model

def init_master_model():
    master_name = get_master_name()
    instance_id = get_master_name()
    local_config =  sys_def.get_system_defaults( 'local_settings', 'Master' )
    comm_queue = local_config['init-queue'] 
    status = master_mdl.INIT 
    inst_md = boto.utils.get_instance_metadata()
    aws_region = inst_md['placement']['availability-zone'][:-1]
    return master_mdl.insert_master( master_name, 
        aws_region=aws_region,
        instance_id = instance_id,
        comm_queue = comm_queue,
        status = status )

def add_run_meta( run_model ):
    run_settings = run_model['run_settings']
    logger = logging.getLogger('add_run_meta')
    logger.info('Adding metadata for this run')
    conn = boto.dynamodb2.connect_to_region( 'us-east-1' )
    run_meta_table = run_settings[ 'run_meta_table' ]
    run_id = run_model['run_id']
    #TODO: put this in a model
    table = Table( run_meta_table, connection = conn )
    if table.query_count( run_id__eq=run_id ) == 0:
        timestamp = datetime.datetime.utcnow().strftime('%Y.%m.%d-%H:%M:%S')
        item = Item( table, data={'run_id':run_id,  'timestamp':timestamp} )
        item['config'] = base64.b64encode( json.dumps(  run_model  ) )
        #k is in config, and that will be what most queries are interested in
        #so no need to make all clients decode64 and load json
        item['k'] = run_settings[ 'k' ]
        item.save()
    return run_id

def get_work( run_model, perm=True ):
    """
    Returns list of tuples of the form
    (strain, num_runs, shuffle, k)
    Going to have to modify the data size part if allowing k to float
    """
    import masterdirac.models.systemdefaults as sys_def
    local_config =  sys_def.get_system_defaults('local_settings', 'Master')
    source_data = run_model['source_data']
    run_settings = run_model['run_settings']
    md = datadirac.data.MetaInfo( os.path.join(
        local_config[ 'working_dir' ],
        source_data[ 'meta_file' ] ) )
    p = run_settings[ 'permutations' ]
    k = run_settings[ 'k' ]
    strains = md.get_strains()
    run_id = add_run_meta( run_model )
    if perm:
        return [(run_id, strain, p, True, k) for strain in md.get_strains()]
    else:
        return [(run_id, strain, 1, False, k) for strain in md.get_strains() ]

def run( data_sizes, master_model, run_model):

    logger = logging.getLogger("MasterServer.run")
    logger.info("Getting work")
    #work = get_work( config, perm = False )
    logger.info("Creating Server Manager")
    master_mdl.insert_master( master_model['master_name'], 
            status = master_mdl.RUN )
    
    manager = controller.serverinterface.ServerManager(data_sizes,
            master_model, run_model )
    logger.info("Adding work to manager")
    manager.add_work( get_work(run_model, perm=False) )
    manager.add_work( get_work(run_model, perm=True) )
    terminate = False
    while not terminate:
        logger.debug("Starting work cycle")
        manager.poll_launcher( timeout=5 )
        manager.poll_for_server( timeout=5 )
        if manager.has_work():
            manager.send_run()
        terminate = manager.inspect()
        logger.debug("Ending work cycle")
    #manager.terminate_data_servers()
    logger.info("Exiting run")

def get_run( master_model ):
    """
    Look for active/scheduled runs
    """
    import masterdirac.models.run as run
    logger = logging.getLogger("MasterServer.get_run")
    logger.debug("Looking for current run")
    master_name = master_model['master_name'] 
    for item in run.ANRun.scan():
        if item.status in [run.INIT, run.ACTIVE]:
            this_run = run.to_dict( item )
            if (this_run['status'] == run.INIT 
            or this_run.master_name == master_name):
                if not this_run['master_name']:
                    this_run = run.insert_ANRun( this_run['run_id'],
                            master_name=master_name)
                logger.info( "Found run %r" % this_run )
                return this_run
    logger.info("No run found")
    return None

def main():
    from masterdirac.utils import debug
    name = 'MasterServer'
    debug.initLogging()
    logger = logging.getLogger(name)

    master_model = init_infrastructure()
    logger.info( "Current model %r" % master_model )
    run_model = get_run( master_model )
    while run_model is None:
        logger.warning("No run available") 
        time.sleep( 10 )
        run_model = get_run( master_model )
        #need to get a term signal somewhere
    data_sizes = init_data( run_model )
    run( data_sizes, master_model, run_model )
    master_mdl.insert_master( master_model['master_name'], 
            status = master_mdl.TERMINATED )

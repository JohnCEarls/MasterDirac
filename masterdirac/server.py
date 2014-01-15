import boto
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
def init_data( config ):
    """
    Grabs data sources from s3, creates the dataframe needed
    by the system and moves it to another s3 bucket
    """
    args = ( config.get('source_data', 'bucket'),
            config.get('source_data', 'data_file'),
            config.get('source_data', 'meta_file'),
            config.get('source_data', 'annotations_file'),
            config.get('source_data', 'synonym_file'),
            config.get('source_data', 'agilent_file'),
            config.get('local_settings', 'working_dir') )
    logger = logging.getLogger('DataInit')
    logger.info("Getting source data")

    hddata_process.get_from_s3( *args)
    logger.info("Generating dataframe")
    hddg = hddata_process.HDDataGen(config.get('local_settings','working_dir'))
    df,net_est = hddg.generate_dataframe( config.get('source_data', 'data_file'),
                                config.get('source_data', 'annotations_file'),
                                config.get('source_data', 'agilent_file'),
                                config.get('source_data', 'synonym_file'),
                                config.get('network_config', 'network_table'),
                                config.get('network_config', 'network_source'))
    logger.info("Sending dataframe to s3://%s/%s" % ( 
        config.get('dest_data', 'working_bucket'), 
        config.get('dest_data', 'dataframe_file') ) )
    hddg.write_to_s3( config.get('dest_data', 'working_bucket'), df, 
            config.get('dest_data', 'dataframe_file') )
    conn = boto.connect_s3()
    source = conn.get_bucket( config.get('source_data', 'bucket') )
    k = Key(source)
    k.key = config.get('source_data', 'meta_file')
    k.copy( config.get('dest_data', 'working_bucket'), k.name )
    logger.info("Sending metadata file to s3://%s/%s" % (
                    config.get('dest_data', 'working_bucket'), k.name ))
    max_nsamples = len(df.columns)
    max_ngenes = len(df.index)
    max_nnets = len(net_est)
    max_comp = sum([x*(x-1)/2 for x in net_est])
    logger.debug( "UB on dims4 samp[%i], gen[%i],nets[%i], comp[%s]" % (
                max_nsamples, max_ngenes, max_nnets, max_comp) )
    return (max_nsamples, max_ngenes, max_nnets, max_comp)

def init_infrastructure( config ):
    logger = logging.getLogger('init_infrastructure')
    #make sure init queue is available
    conn = boto.sqs.connect_to_region( 'us-east-1' )
    q = conn.create_queue( 
            config.get('run_settings', 'server_initialization_queue') )
    if not q:
        logger.error("Init Q not created")
        raise Exception("Unable to initialize Init Q")

def add_run_meta( config ):
    logger = logging.getLogger('add_run_meta')
    logger.info('Adding metadata for this run')
    conn = boto.dynamodb2.connect_to_region( 'us-east-1' )
    run_meta_table = config.get('run_settings', 'run_meta_table')
    run_id = get_run_id( config )
    table = Table( run_meta_table, connection = conn )
    if table.query_count( run_id__eq=run_id ) == 0:
        timestamp = datetime.datetime.utcnow().strftime('%Y.%m.%d-%H:%M:%S')
        item = Item( table, data={'run_id':run_id,  'timestamp':timestamp} )
        item['config'] = base64.b64encode( json.dumps( config._sections ) )
        #k is in config, but this will be what most queries are interested in
        #so no need to make all clients decode64 and load json
        item['k'] = config.get('run_settings', 'k')
        item.save()
    return run_id

def get_run_id( config ):
    r_id = config.get('run_settings', 'run_id')
    if not r_id:
        #randomly generate run_id
        r_id = ''.join(random.choice(string.ascii_lowercase) 
                for x in range(5))
    return r_id

def get_work( config, perm=True ):
    """
    Returns list of tuples of the form
    (strain, num_runs, shuffle, k)
    Going to have to modify the data size part if allowing k to float
    """
    md = datadirac.data.MetaInfo( os.path.join(
        config.get('local_settings', 'working_dir'),
        config.get('source_data', 'meta_file') ) )
    p = config.getint('run_settings', 'permutations')
    k = config.getint('run_settings', 'k')
    strains = md.get_strains()
    run_id = add_run_meta( config )
    if perm:
        return [(run_id, strain, p, True, k) for strain in md.get_strains()]
    else:
        return [(run_id, strain, 1, False, k) for strain in md.get_strains()]

def run( data_sizes, config):
    logger = logging.getLogger("MasterServer.run")
    logger.info("Getting work")
    #work = get_work( config, perm = False )
    logger.info("Creating Server Manager")
    manager = controller.serverinterface.ServerManager(data_sizes,config)
    logger.info("Adding work to manager")
    manager.add_work( get_work(config, perm=False) )
    manager.add_work( get_work(config, perm=True) )
    while manager.has_work():
        logger.debug("Starting work cycle")
        manager.poll_for_server()
        manager.send_run()
        manager.inspect()
        logger.debug("Ending work cycle")
    manager.terminate_data_servers()
    logger.info("Exiting run")

def main():
    import argparse
    import ConfigParser 
    from masterdirac.utils import debug
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Configfile name', required=True)
    args = parser.parse_args()
    name = 'MasterServer'
    debug.initLogging( args.config )
    logger = logging.getLogger(name)
    logger.info("Getting config[%s]" % args.config)
    config = ConfigParser.ConfigParser()
    config.read( args.config )
    init_infrastructure( config )
    data_sizes = init_data( config )
    run( data_sizes, config)

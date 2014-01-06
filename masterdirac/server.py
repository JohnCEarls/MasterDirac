import boto
from boto.s3.key import Key
from utils import hddata_process
import numpy as np
import datadirac.data
import os.path
import logging
import controller.serverinterface
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


def get_work( config ):
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
    return [(strain, p, True, k) for strain in md.get_strains()]

def run( data_sizes, config):
    logger = logging.getLogger("MasterServer.run")
    logger.info("Getting work")
    work = get_work( config )
    logger.warning("*"*17)
    logger.warning("*DEBUG CODE HERE*")
    logger.warning("*Only creating one strain*")
    logger.warning("*"*17)
    work = work[:1]
    logger.info("Creating Server Manager")
    manager = controller.serverinterface.ServerManager(data_sizes,config)
    logger.info("Adding work to manager")
    manager.add_work( work )
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

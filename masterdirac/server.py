import boto
from utils import hddata_process
import numpy as np

def init_data( config ):
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
    max_nsamples = len(df.columns)
    max_ngenes = len(df.index)
    max_nnets = len(net_est)
    max_comp = sum([x*(x-1)/2 for x in net_est])
    return (max_nsamples, max_ngenes, max_nnets, max_comp)

def get_work():
    """
    Returns list of tuples of the form
    (strain, num_runs, shuffle, k)
    Going to have to modify the data size part if allowing k to float
    """
    return [('129', 1000, True, 5)] 

def run( data_sizes, config):
    work = get_work()
    manager = controller.serverinterface.ServerManager(data_sizes,config)
    manager.add_work( work )
    while manager.has_work():
        manager.poll_for_server()
        manager.send_run()
        manager.inspect()

def main():
    import argparse
    import ConfigParser 
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Configfile name', required=True)
    args = parser.parse_args()
    name = 'MasterServer'
    debug.initLogging( args.config )
    logger = logging.getLogger(name)
    logger.info("Getting config[%s]" % args.config)
    config = ConfigParser.ConfigParser( args.config )
    data_sizes = init_data( config )
    run( data_sizes, config)

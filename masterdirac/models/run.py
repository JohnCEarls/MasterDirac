from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute)
from datetime import datetime

#STATUS_CODES
INIT = 0
ACTIVE = 10
COMPLETE = 20
ABORT = 30

class ANRun(Model):
    class Meta:
        table_name = 'aurea-nebula-run-config'
        region = 'us-east-1'
    run_id = UnicodeAttribute( hash_key=True )
    master_name = UnicodeAttribute(default='')
    workers = UnicodeSetAttribute( default = [] )
    source_data = JSONAttribute(default={})
    dest_data = JSONAttribute(default={})
    description = UnicodeAttribute(default= '')
    network_config = JSONAttribute(default={})
    run_settings = JSONAttribute( default={} )
    intercomm_settings = JSONAttribute( default={} )
    aggregator_settings = JSONAttribute( default={} )
    date_created = UTCDateTimeAttribute( default = datetime.utcnow() )
    status = NumberAttribute(default=0)

def insert_ANRun( run_id, 
        master_name=None, 
        workers=None, 
        source_data=None,
        dest_data= None,
        description=None, 
        network_config=None, 
        run_settings=None,
        intercomm_settings=None, 
        aggregator_settings=None,
        status=None ):
    item = ANRun( run_id )
    if master_name is not None:
        item.master_name = master_name
    if workers is not None:
        item.workers = workers
    if source_data is not None:
        item.source_data = source_data
    if dest_data is not None:
        item.dest_data = dest_data
    if description is not None:
        item.description = description
    if network_config is not None:
        item.network_config = network_config
    if run_settings is not None:
        item.run_settings = run_settings
    if intercomm_settings is not None:
        item.intercomm_settings = intercomm_settings
    if aggregator_settings is not None:
        item.aggregator_settings = aggregator_settings
    if status is not None:
        item.status = status
    item.save()
    return to_dict(item)

def to_dict( run_item ):
    result = {}
    result['run_id'] = run_item.run_id
    result['master_name'] = run_item.master_name
    result['workers'] = run_item.workers
    result['source_data'] = run_item.source_data
    result['dest_data'] = run_item.dest_data
    result['description'] = run_item.description
    result['network_config'] = run_item.network_config
    result['run_settings'] = run_item.run_settings
    result['intercomm_settings'] = run_item.intercomm_settings
    result['aggregator_settings'] = run_item.aggregator_settings
    result['data_created'] = run_item.date_created
    result['status'] = run_item.status
    return result

def get_ANRun( run_id=None ):
    if run_id is not None:
        result = {}
        try:
            item = ANRun( run_id )
            return to_dict( item )
        except ANRun.DoesNotExist as dne:
            return {}
    else:
        results = []
        for item in ANRun.scan():
            results.append( to_dict( item ) )
        return results

if __name__ == "__main__":
    if not ANRun.exists():
        ANRun.create_table( read_capacity_units=2, write_capacity_units=1,
            wait=True )

    default_source_data = {
            'bucket':'hd_source_data',
            'data_file':'exp_mat_b6_wt_q111.txt',
            'meta_file':' metadata_b6_wt_q111.txt',
            'annotations_file':'annodata_b6.txt',
            'agilent_file':'HDLux_agilent_gene_list.txt',
            'synonym_file':'Mus_homo.gene_info'
    }

    default_dest_data = {
            'working_bucket' : 'hd_working_0',
            'meta_file' : 'metadata_b6_wt_q111.txt',
            'dataframe_file' : 'trimmed_dataframe_b6.pandas'
    }

    default_network_config = {
            'network_table':'net_info_table',
            'network_source':'c2.cp.biocarta.v4.0.symbols.gmt'
    }

    default_run_settings = {
        'run_meta_table':'run_gpudirac_hd',
        'run_truth_table':'truth_gpudirac_hd',
        'run_id':'black_6_biocarta_wt_q111_4',
        'server_initialization_queue':'tcdirac-master',
        'k':11,
        'sample_block_size' : 32,
        'pairs_block_size' : 16,
        'nets_block_size' : 8,
        'heartbeat_interval' : 100,
        'permutations' : 10000,
        'chunksize' : 1000
    }

    default_intercomm_settings = {
        'sqs_from_data_to_gpu':'from-data-to-gpu-bioc',
        'sqs_from_gpu_to_agg':'from-gpu-to-agg-bioc',
        'sqs_from_data_to_agg':'from-data-to-agg-bioc',
        'sqs_from_data_to_agg_truth':'from-data-to-agg-bioc-truth',
        's3_from_data_to_gpu':'ndp-from-data-to-gpu-bioc',
        's3_from_gpu_to_agg':'ndp-from-gpu-to-agg-bioc'
    }

    insert_ANRun( 'default', 
            source_data = default_source_data,
            network_config= default_network_config,
            run_settings = default_run_settings,
            intercomm_settings = default_intercomm_settings,
            status = COMPLETE)


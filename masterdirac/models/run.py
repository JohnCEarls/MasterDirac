from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute)
from datetime import datetime
import json
import collections

#STATUS_CODES
CONFIG = -10
INIT = 0
ACTIVE = 10
ACTIVE_ALL_SENT = 15
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
    data_sizes = UnicodeAttribute( default='' )
    date_created = UTCDateTimeAttribute( default = datetime.utcnow() )
    status = NumberAttribute( default=CONFIG )

class ANRunCheckpoint(Model):
    class Meta:
        table_name = 'aurea-nebula-run-checkpoint'
        region = 'us-east-1'
    run_id = UnicodeAttribute( hash_key=True )
    date_created = UnicodeAttribute( range_key = True )
    num_sent = NumberAttribute( default=0 )
    strain = UnicodeAttribute( default='None' )

def pack_checkpoint( run_id, num_sent, date_created=None, strain=None):
    if date_created is None:
        date_created = datetime.utcnow().isoformat()
    if strain is None:
        strain = 'None'
    return (run_id, num_sent, date_created, strain)

def batch_checkpoint( runs ):
    with ANRunCheckpoint.batch_write() as batch:
        items = [ANRunCheckpoint(run[0], run[2], num_sent=run[1], strain=run[3])  
                    for run in runs]
        for item in items:
            batch.save(item)

def get_checkpoint( run_id ):
    acc = collections.defaultdict(int)
    for item in ANRunCheckpoint.query( run_id ):
        acc[ item.strain ] += item.num_sent
    return acc

def compress_checkpoint( run_id ):
    checkpoint = get_checkpoint(run_id)
    cp = []
    delete_checkpoint( run_id )
    for strain, num_sent in checkpoint.items():
        cp.append(pack_checkpoint( run_id, num_sent, strain=strain ))
    batch_checkpoint( cp )

def delete_checkpoint( run_id ):
    with ANRunCheckpoint.batch_write() as batch:
        for item in ANRunCheckpoint.query( run_id ):
            batch.delete(item)

def update_ANRun( run_id, 
        master_name=None, 
        workers=None, 
        source_data=None,
        dest_data= None,
        description=None, 
        network_config=None, 
        run_settings=None,
        intercomm_settings=None, 
        aggregator_settings=None,
        status=None,
        data_sizes=None
    ):
    item = ANRun.get( run_id )
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
        if 'run_id' in run_settings:
            run_settings['run_id'] = run_id
        item.run_settings = run_settings
    if intercomm_settings is not None:
        item.intercomm_settings = intercomm_settings
    if aggregator_settings is not None:
        item.aggregator_settings = aggregator_settings
    if status is not None:
        item.status = status
    if data_sizes is not None:
        if type(data_sizes) is tuple:
            data_sizes = json.dumps( data_sizes )
        item.data_sizes = data_sizes
    item.save()
    return to_dict(item)

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
        status=None,
        data_sizes=None
        ):
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
        if 'run_id' in run_settings:
            run_settings['run_id'] = run_id
        item.run_settings = run_settings
    if intercomm_settings is not None:
        item.intercomm_settings = intercomm_settings
    if aggregator_settings is not None:
        item.aggregator_settings = aggregator_settings
    if status is not None:
        item.status = status
    if data_sizes is not None:
        if type(data_sizes) is tuple:
            data_sizes = json.dumps( data_sizes )
        item.data_sizes = data_sizes
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
    result['date_created'] = run_item.date_created
    result['status'] = run_item.status
    if run_item.data_sizes:
        result['data_sizes'] = json.loads( run_item.data_sizes )
    else:
        result['data_sizes'] = None
    return result

def get_ANRun( run_id=None ):
    if run_id is not None:
        result = {}
        try:
            item = ANRun.get( run_id )
            return to_dict( item )
        except ANRun.DoesNotExist as dne:
            return {}
    else:
        results = []
        for item in ANRun.scan():
            results.append( to_dict( item ) )
        return results

def get_pending_ANRun():    
        results = get_ANRun()
        return [result for result in results if result['status'] in [CONFIG]]

def get_active_ANRun( run_id=None, master_id=None):
    if run_id is not None:
        return get_ANRun( run_id )
    elif master_id is not None:
        for result in get_ANRun():
            if result['master_id'] == master_id:
                return result
        return {}
    else:
        results = get_ANRun()
        return [result for result in results if result['status'] in [INIT, ACTIVE]]

def delete_ANRun( run_id ):
    item = ANRun.get( run_id )
    item.delete()

def test_checkpoint():
    try:
        import random
        assert len(get_checkpoint('nonexistent')) == 0, "Returning nonexistent records"
        test_run_id = 'mytest-%i' % random.randint(0,100)
        N = 100
        M = 10
        cp = []
        strains =  ['strain1', 'strain2']
        for i in range(N):
            for strain in strains:
                cp.append( pack_checkpoint( run_id=test_run_id, strain=strain, num_sent=M) )
        batch_checkpoint( cp )
        cp = get_checkpoint( test_run_id )
        for a,b in zip(strains, strains):
            assert cp[a] == cp[b]
        assert cp[strains[0]] == M*N
        compress_checkpoint( test_run_id )
        cp2 = get_checkpoint( test_run_id )
        for strain in strains:
            assert cp[strain] == cp2[strain]
        #cleanup
        delete_checkpoint( test_run_id )
        assert  len(get_checkpoint(test_run_id)) == 0, "Returning nonexistent records"
    except AssertionError as ae:
        print "Test failed[test_checkpoint]"
        raise
    print "Tests Passed[test_checkpoint]"

if __name__ == "__main__":
    if not ANRun.exists():
        ANRun.create_table( read_capacity_units=2, write_capacity_units=1,
            wait=True )
    """
    default_source_data = {
            'bucket':'hd_source_data',
            'data_file':'exp_mat_b6_wt_q111.txt',
            'meta_file':'metadata_b6_wt_q111.txt',
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
            dest_data = default_dest_data,
            network_config= default_network_config,
            run_settings = default_run_settings,
            intercomm_settings = default_intercomm_settings,
            status = COMPLETE)"""


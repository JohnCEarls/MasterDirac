from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute, BooleanAttribute
        )
from datetime import datetime
import hashlib
import logging
import boto.ec2
import boto.exception
#STATES
NA = -10
TERMINATED_WITH_ERROR = -5
CONFIG = 0
STARTING = 10
READY = 20
RUNNING = 30
MARKED_FOR_TERMINATION = 35
TERMINATING = 37
TERMINATED = 40

active_statuses = [CONFIG, STARTING, READY, RUNNING, MARKED_FOR_TERMINATION]

logger = logging.getLogger(__name__)

#worker defaults

class ANWorker(Model):
    """
    """
    class Meta:
        table_name='aurea-nebula-worker'
        region = 'us-east-1'
    worker_id = UnicodeAttribute( hash_key=True )
    master_name = UnicodeAttribute( default='' )
    cluster_name = UnicodeAttribute( default=''  )
    cluster_type = UnicodeAttribute(default='data')
    date_created = UTCDateTimeAttribute( default=datetime.utcnow() )
    aws_region = UnicodeAttribute(default='')
    num_nodes = NumberAttribute(default=0)
    nodes = UnicodeSetAttribute(default=[])
    status = NumberAttribute(default=0)
    starcluster_config = JSONAttribute(default={})
    startup_log = UnicodeAttribute(default='')
    startup_pid = UnicodeAttribute(default='')
    key = UnicodeAttribute(default = '')


def to_dict_ANW( item ):
    """
    Convert workerbase to dictionary
    """
    result = {}
    for key, value in item.attribute_values.iteritems():
        result[key] = value
        if type(value) == set:
            result[key] = list(value)
    return result

def insert_ANWorker( master_name, cluster_name,
            cluster_type,
            aws_region,
            num_nodes = None,
            status = None,
            starcluster_config = None,
            startup_log = None,
            startup_pid = None,
            key = None,
        ):
    date = datetime.utcnow().isoformat()
    key_base = '-'.join([master_name, cluster_name,cluster_type, aws_region, date])
    m = hashlib.md5()
    m.update( key_base )#just a key, not meant to be secure, just unique
    worker_id = m.hexdigest()
    item = ANWorker( worker_id )
    item.master_name = master_name
    item.cluster_name = cluster_name
    item.cluster_type = cluster_type
    item.aws_region = aws_region

    if num_nodes is not None:
        item.num_nodes = num_nodes
    if status is not None:
        item.status = status
    if starcluster_config is not None:
        item.starcluster_config = starcluster_config
    if startup_log is not None:
        item.startup_log = startup_log
    if startup_pid is not None:
        item.startup_pid = startup_pid
    if key is not None:
        item.key = key
    item.save()
    return to_dict_ANW(item)



def update_ANWorker( worker_id,
            num_nodes = None,
            nodes = None,
            status = None,
            starcluster_config = None,
            startup_log = None,
            startup_pid = None,
            key = None,
        ):
    item = ANWorker.get( worker_id )

    if num_nodes is not None:
        item.num_nodes = num_nodes
    if status is not None:
        item.status = status
    if starcluster_config is not None:
        item.starcluster_config = starcluster_config
    if startup_log is not None:
        item.startup_log = startup_log
    if startup_pid is not None:
        item.startup_pid = startup_pid
    if key is not None:
        item.key = key
    if nodes is not None:
        item.nodes = nodes
    item.save()
    return to_dict_ANW( item )

def get_ANWorker( worker_id=None, master_name=None, cluster_name=None ):
    """
    If a worker_id is given, this returns a single Worker,
        otherwise a list of matching workers is returned
    """
    if worker_id is not None:
        return _get_ANWorker( worker_id )
    else:
        results = []
        for item in ANWorker.scan():#pynamo screwed up scan
            #TODO: fix pynamo or wait for fix
            if master_name is not None and item.master_name != master_name:
                continue
            if cluster_name is not None and item.cluster_name != cluster_name:
                continue
            results.append( to_dict_ANW( item ) )
        return results

def get_active_workers():
    """
    Returns workers that may be operated on (i.e. have not been used)
    """
    results = []
    for item in ANWorker.scan():
        if item.status in active_statuses:
            results.append( to_dict_ANW( item ) )
    results.sort( key=lambda x: (x['cluster_type'], x['aws_region'], x['status']) )
    return results

def confirm_worker_running( worker ):
    """
    Checks that at least one node in the cluster is running
    """
    try:
        conn = boto.ec2.connect_to_region( worker['aws_region'] )
        for reservation in conn.get_all_reservations( 
                                  instance_ids=list(worker['nodes']) ):
            for inst in reservation.instances:
                if inst.state == 'running':
                    return True
    except AttributeError as ae:
        logger.exception("Unable to connect to [%s] region." % (
            worker['aws_region'] ))
        logger.error("Error attempting to confirm [%r]" % ( worker ))
    except boto.exception.EC2ResponseError as ec2e:
        logger.exception("%s is an invalid instance id in %s region." % (
            worker['instance_id'], worker['aws_region'] ))
        logger.error("Error attempting to confirm [%r]" % ( worker ))
    return False

def _get_ANWorker( worker_id ):
    """
    Gets full record
    """
    def to_dict( item ):
        """
        Convert workerbase to dictionary
        """
        result = {}
        result['cluster_type'] = item.cluster_type
        result['aws_region'] = item.aws_region
        for key, value in item.attribute_values.iteritems():
            result[key] = value
        return result
    try:
        item = ANWorker.get( worker_id )
        return to_dict( item )
    except ANWorker.DoesNotExist as dne:
        return {}

class ANWorkerBase(Model):
    class Meta:
        table_name = 'aurea-nebula-worker-base'
        region = 'us-east-1'
    cluster_type = UnicodeAttribute( hash_key=True )
    aws_region = UnicodeAttribute( range_key=True )
    instance_type = UnicodeAttribute( default='' )
    image_id = UnicodeAttribute( default='' )
    cluster_size = NumberAttribute( default=0 )
    plugins = UnicodeAttribute( default='' )
    force_spot_master = BooleanAttribute( default=True )
    spot_bid = NumberAttribute( default = 0.0 )
    prefix = UnicodeAttribute( default='')
    iam_profile = UnicodeAttribute( default='' )

def insert_ANWorkerBase( cluster_type, aws_region,
        instance_type=None,
        image_id=None,
        cluster_size=None,
        plugins=None,
        force_spot_master=None,
        spot_bid=None,
        prefix=None,
        iam_profile=None
        ):
    item = ANWorkerBase( cluster_type, aws_region)
    if instance_type is not None:
        item.instance_type = instance_type
    if image_id is not None:
        item.image_id = image_id
    if cluster_size is not None:
        item.cluster_size = cluster_size
    if plugins is not None:
        item.plugins = plugins
    if force_spot_master is not None:
        item.force_spot_master = force_spot_master
    if spot_bid is not None:
        item.spot_bid = spot_bid
    if prefix is not None:
        item.prefix = prefix
    if iam_profile is not None:
        item.iam_profile = iam_profile
    item.save()

def update_ANWorkerBase( cluster_type, aws_region, instance_type=None,
        image_id=None, cluster_size=None, plugins=None,
        force_spot_master=None, spot_bid=None):
    item = ANWorkerBase.get( cluster_type, aws_region)
    if instance_type is not None:
        item.instance_type = instance_type
    if image_id is not None:
        item.image_id = image_id
    if cluster_size is not None:
        item.cluster_size = cluster_size
    if plugins is not None:
        item.plugins = plugins
    if force_spot_master is not None:
        item.force_spot_master = force_spot_master
    if spot_bid is not None:
        item.spot_bid = spot_bid
    if prefix is not None:
        item.prefix = prefix
    if iam_profile is not None:
        item.iam_profile = iam_profile
    item.save()

def delete_ANWorkerBase( cluster_type, aws_region ):
    item = ANWorkerBase.get( cluster_type, aws_region)
    item.delete()


def get_ANWorkerBase( cluster_type=None, aws_region=None):
    """
    if cluster_type and aws_region given, returns 1 dict,
    otherwise a list of dicts containing results

    if no results found returns empty list or dict
    """
    def to_dict( item ):
        """
        Convert workerbase to dictionary
        """
        result = {}
        result['cluster_type'] = item.cluster_type
        result['aws_region'] = item.aws_region
        for key, value in item.attribute_values.iteritems():
            result[key] = value
        return result

    if cluster_type is None:
        results = []
        for item in ANWorkerBase.scan():
            if aws_region is None or item.aws_region == aws_region:
                results.append( to_dict( item ) )
        return results
    elif aws_region is None:
        results = []
        for item in ANWorkerBase.query(cluster_type):
            results.append( to_dict( item ) )
        return results
    else:
        try:
            item = ANWorkerBase.get(cluster_type, aws_region)
            return to_dict(item)
        except ANWorkerBase.DoesNotExist as dne:
            return {}

if __name__ == "__main__":
    if not ANWorker.exists():
        ANWorker.create_table( read_capacity_units=2,
            write_capacity_units=1, wait=True)
    if not ANWorkerBase.exists():
        ANWorkerBase.create_table( read_capacity_units=2,
            write_capacity_units=1, wait=True)
    #insert_ANWorkerBase( cluster_type='test', aws_region='test')
    test_data = {'master_name' : 'test-master',
            'cluster_name' : 'test-data-0',
            'cluster_type' : 'Data Cluster',
            'aws_region' : 'us-east-1',
            'num_nodes' : 10,
            'starcluster_config' :{
                'cluster_name':'dummy-cluster',
                'aws_region':'us-east-1',
                'key_name': 'somekey',
                'key_location': '/home/sgeadmin/somekey.key',
                'cluster_size': 1,
                'node_instance_type': 'm1.xlarge',
                'node_image_id': 'ami-1234567',
                'iam_profile':'some-profile',
                'force_spot_master':True,
                'spot_bid':2.00,
                'plugins':'p1,p2,p3'
            },
            'startup_log': 'somelog',
            'key': 'somekey',
            'status': CONFIG,
            }
    insert_ANWorker( **test_data )

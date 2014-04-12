from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute, BooleanAttribute
        )
from datetime import datetime
import hashlib
#STATES
OFF = 0
LAUNCHING = 0
UNINITIALIZED = 1
READY = 2
TERMINATED = 3

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
    state = NumberAttribute(default=0)
    starcluster_config = JSONAttribute(default={})
    startup_log = UnicodeAttribute(default='')
    startup_pid = UnicodeAttribute(default='')
    key = UnicodeAttribute(default = '')
    logging_config = JSONAttribute( default={} )
    cluster_init_config = JSONAttribute( default={} )

def insert_ANWorker( master_name, cluster_name ):
    date = datetime.utcnow()
    key_base = master_name + cluster_name + date
    m = hashlib.md5()
    m.update( key_base )#just a key, not meant to be secure, just unique
    key = m.hexdigest()
    item = ANWorker( key )
    item.master_name = master_name
    item.cluster_name = cluster_name
    item.date_created = date_created


def get_ANWorker( worker_id=None, master_name=None, cluster_name=None ):
    """
    If a worker_id is given, this returns a single Worker,
        otherwise a list of matching workers is returned
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
    if worker_id is not None:
        return _get_ANWorker( worker_id )
    else:
        results = []
        scan_fltr = {}
        if cluster_name is not None:
            scan_fltr['cluster_name__eq'] = cluster_name
        if master_name is not None:
            scan_fltr['master_name__eq'] = master_name
        for item in ANWorkerBase.scan(**scan_fltr):
            results.append( to_dict( item ) )
        return results


def _get_ANWorker( worker_id ):
    """
    Gets full record
    """
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
    insert_ANWorkerBase( cluster_type='test', aws_region='test')

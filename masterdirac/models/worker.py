from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute, BooleanAttribute
        )
from pynamodb.exceptions import DoesNotExist
from datetime import datetime

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
    master_name = UnicodeAttribute( hash_key=True )
    cluster_name = UnicodeAttribute( range_key=True )
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

def insert_ANWorkerBase( cluster_type, aws_region, instance_type=None,
        image_id=None, cluster_size=None, plugins=None,
        force_spot_master=None, spot_bid=None):
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

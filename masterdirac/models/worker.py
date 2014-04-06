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
    table_name='aurea-nebula-worker'
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
    table_name = 'aurea-nebula-worker-base'
    cluster_type = UnicodeAttribute( hash_key=True )
    aws_region = UnicodeAttribute( range_key=True )
    instance_type = UnicodeAttribute( default='' )
    image_id = UnicodeAttribute( default='' )
    cluster_size = NumberAttribute( default='' )
    plugins = UnicodeSetAttribute( default=[] )
    force_spot_master = BooleanAttribute( default=True )
    spot_bid = NumberAttribute( default = 0.0 )

if __name__ == "__main__":
    if not ANWorker.exists():
        ANWorker.create_table( read_capacity_units=2,
            write_capacity_units=1, wait=True)
    if not ANWorkerBase.exists():
        ANWorkerBase.create_table( read_capacity_units=2,
            write_capacity_units=1, wait=True)

from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute)
from datetime import datetime

#STATES
NA = -10
INIT = 0
RUN = 10
TERMINATED = 30


class ANMaster(Model):
    class Meta:
        region = 'us-east-1'
        table_name = 'aurea-nebula-master'
    master_name = UnicodeAttribute( hash_key=True )
    date_created = UTCDateTimeAttribute(
            default=datetime.utcnow() )
    aws_region = UnicodeAttribute(default='')
    key_pairs = JSONAttribute(default={})
    instance_id = UnicodeAttribute(default='')
    comm_queue = UnicodeAttribute(default='')
    status = NumberAttribute(default=0)

def to_dict( master_item ):
    result = {}
    result['master_name'] = master_item.master_name
    result['date_created'] = master_item.date_created
    result['aws_region'] = master_item.aws_region
    result['key_pairs'] = master_item.key_pairs
    result['instance_id'] = master_item.instance_id
    result['comm_queue'] = master_item.comm_queue
    result['status'] = master_item.status
    return result

def get_active_master( ):
    masters = get_master()
    for master in masters:
        if master['status'] in [INIT, RUN]:
            return master
    return None


def get_master( master_name=None):
    if master_name is None:
        results = []
        for item in ANMaster.scan():
            results.append( to_dict( item ) )
        return results
    else:
        try:
            print "in get_master"
            print master_name
            item = ANMaster.get( master_name )
            return to_dict( item )
        except ANMaster.DoesNotExist as dne:
            return {}

def insert_master( master_name, 
        aws_region=None,
        key_pairs=None,
        instance_id = None,
        comm_queue = None,
        status = None ):
    item = ANMaster( master_name )
    if aws_region is not None:
        item.aws_region = aws_region
    if key_pairs is not None:
        item.key_pairs = key_pairs
    if instance_id is not None:
        item.instance_id = instance_id
    if comm_queue is not None:
        item.comm_queue = comm_queue
    if status is not None:
        item.status = status
    item.save()
    return to_dict( item )


if __name__ == "__main__":
    if not ANMaster.exists():
        print "creaing aurea-nebula-master"
        ANMaster.create_table( read_capacity_units=2, 
            write_capacity_units=1, wait=True )


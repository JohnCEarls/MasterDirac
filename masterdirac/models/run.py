from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute)
from datetime import datetime

class ANRun(Model):
    class Meta:
        table = 'aurea-nebula-run-config'
        region = 'us-east-1'
    run_id = UnicodeAttribute( hash_key=True )
    master_name = UnicodeAttribute(default='')
    workers = UnicodeSetAttribute( default = [] )
    source_data = JSONAttribute(default={})
    description = UnicodeAttribute(default= '')
    network_config = JSONAttribute(default={})
    run_settings = JSONAttribute( default={} )
    intercomm_settings = JSONAttribute( default={} )
    aggregator_settings = JSONAttribute( default={} )
    date_created = UTCDateTimeAttribute( default = datetime.utcnow() )

def insert_ANRun( run_id, master_name=None, workers=None, source_data=None,
        description=None, network_config=None, run_settings=None,
        intercomm_settings=None, aggregator_settings=None):
    item = ANRun( run_id )
    if master_name is not None:
        raise Exception("Run is not implemented")


if __name__ == "__main__":
    if not ANRun.exists():
        ANRun.create_table( read_capacity_units=2, write_capacity_units=1,
            wait=True )

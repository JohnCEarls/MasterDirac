from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute)
from pynamodb.exceptions import DoesNotExist
from datetime import datetime

class ANRun(Model):
    table = 'aurea-nebula-run-config'
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

if __name__ == "__main__":
    if not ANRun.exists():
        ANRun.create_table( read_capacity_units=2, write_capacity_units=1,
            wait=True )

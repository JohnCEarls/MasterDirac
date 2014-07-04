from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute, BooleanAttribute
        )
from pynamodb.models import Model
from datetime import datetime
import hashlib
import logging
import boto.ec2
import boto.exception
import masterdirac.models.master as master_mdl

logger = logging.getLogger(__name__)

INIT = 0
STARTING = 10
RESTARTING = 20
RUNNING = 30
WAITING = 35
TERMINATING = 40
TERMINATED = 50

class ANServer(Model):
    class Meta:
        table_name='aurea-nebula-server'
        region='us-east-1'
    worker_id = UnicodeAttribute( hash_key=True )
    server_id = UnicodeAttribute( range_key=True )
    status = NumberAttribute( default=0 )

def insert_ANServer( worker_id, server_id, status=0):
    item = ANServer( worker_id, server_id)
    item.status = status
    item.save()
    return to_dict_ANS( item )

def update_ANServer( worker_id, server_id, status):
    item = ANServer( worker_id, server_id)
    item.status = status
    item.save()
    return to_dict_ANS( item )

def get_ANServer( worker_id, server_id=None):
    if server_id is not None:
        return to_dict_ANS( ANServer.get(worker_id, server_id) )
    else:
        return [to_dict_ANS(s) for s in ANServer.query( worker_id )]

def get_status( worker_id, server_id ):
    item = ANServer.get(worker_id, server_id)
    return item.status

def to_dict_ANS( item ):
    d = {}
    d['worker_id'] = item.worker_id
    d['server_id'] = item.server_id
    d['status'] = item.status
    return d

if __name__ == "__main__":
    if not ANServer.exists():
        ANServer.create_table( read_capacity_units=2,
            write_capacity_units=1, wait=True)

    r = insert_ANServer( 'insert-test', 'it-1' )
    assert r['worker_id'] == 'insert-test'
    assert r['server_id'] == 'it-1'
    assert r['status'] == INIT 
    assert len( get_ANServer( 'insert-test') ) > 0
    gotten = get_ANServer( 'insert-test', 'it-1')
    for k in r.iterkeys():
        assert r[k] == gotten[k]


    r = update_ANServer('insert-test', 'it', STARTING)
    assert r['worker_id'] == 'insert-test'
    assert r['server_id'] == 'it'
    assert r['status'] == STARTING

    assert r['status'] == get_status( 'insert-test', 'it' )

    r = update_ANServer('insert-test', 'it', TERMINATED)
    


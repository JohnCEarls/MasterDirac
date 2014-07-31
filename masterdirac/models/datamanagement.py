from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute, BooleanAttribute
        )
from datetime import datetime
import logging
import boto.ec2
import boto.exception
import masterdirac.models.master as master_mdl
import hashlib
import random

#LOCATION_ID
ISB = 0
WWU = 10
S3 = 100

class ANFileObject(Model):
    class Meta:
        table_name='aurea-nebula-file-object'
        region='us-east-1'
    file_object_id = UnicodeAttribute( hash_key=True )
    name = UnicodeAttribute( default='' )
    description = UnicodeAttribute( default='' )
    location = JSONAttribute( default={} )
    metadata = JSONAttribute( default={} )
    md5 = UnicodeAttribute( default='')
    date_created=UTCDateTimeAttribute( default=datetime.utcnow() )

class ANProcessObject(Model):
    class Meta:
        table_name='aurea-nebula-process-object'
        region='us-east-1'
    process_object_id = UnicodeAttribute( hash_key=True )
    name = UnicodeAttribute( default='' )
    description = UnicodeAttribute( default='' )
    location = JSONAttribute( default={} )
    metadata = JSONAttribute( default={} )
    date_created = UTCDateTimeAttribute( default=datetime.utcnow() )

def add_s3_file(bucket_name, key,  filename=None, 
        description=None, process_object_id=None, 
        process_git_hash=None, 
        source_files=[], metadata={} ):
    location = {'loc_id': S3,
            'bucket': bucket_name,
            'key' : key }
    conn = boto.connect_s3()
    b = conn.get_bucket( bucket_name )
    k = b.get_key( key )
    md5 = k.md5
    if filename is None:
        _, filename = os.path.split( key )
    file_object_id = hashlib.md5('%s%i'%(md5,random.randint(0,9999)).hexdigest())
    item = ANFileObject(file_object_id)
    item.name = filename
    if description:
        item.description = description
    item.location = location
    metadata = metadata
    if process_object_id:
       metadata['process_object_id'] = process_object_id
    if process_git_hash:
       metadata['process_git_id'] = process_git_id
    if source_files:
       metadata['source_files'] = source_files
    item.metadata = metadata
    item.save()
    return file_object_id



if __name__ == "__main__":

    #TODO: test
    

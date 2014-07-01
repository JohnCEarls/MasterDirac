import boto
from boto.exception import  S3ResponseError
from boto.s3.lifecycle import Lifecycle,Expiration
deletable = ['ndp-from-gpu-to-agg-bioc-test0', 'diraclog-dev']
def delete(bucket_name):
    if bucket_name in deletable:
        return True
    prefix = 'an-from-data-to-gpu-'
    if bucket_name[:len(prefix)] == prefix:
        return True
    prefix = 'an-from-gpu-to-agg-'
    if bucket_name[:len(prefix)] == prefix:
        return True
    if 'test' in bucket_name.split('-'):
        return True
    return False
s3 = boto.connect_s3()
del_all_pattern = '%s-lc-delete-all'
for b in s3.get_all_buckets():
    if delete( b.name ):
        print b.name
        try:
            config = b.get_lifecycle_config()
            for r in config:
                if r.id == del_all_pattern % b.name:
                    if len(b.get_all_keys()) > 0:
                        print "Want to delete %s but not empty" % b.name
                        print "Try again tomorrow"
                    else:
                        b.delete()
        except S3ResponseError as sre:
            continue
            if sre.error_code == 'NoSuchLifecycleConfiguration':
                print "Setting deletion lifecycle rule"
                lf = Lifecycle()
                lf.add_rule( id=del_all_pattern % b.name, expiration=Expiration(days=1),
                        prefix='', status='Enabled',transition=None  )
                b.configure_lifecycle(lf)


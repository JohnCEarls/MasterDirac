import boto
from boto.exception import  S3ResponseError
from boto.s3.lifecycle import Lifecycle,Expiration
def delete(bucket_name):
    if bucket_name == 'ndp-from-data-to-gpu-bioc-test0':
        return True
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
            if sre.error_code == 'NoSuchLifecycleConfiguration':
                print "Setting deletion lifecycle rule"
                lf = Lifecycle()
                lf.add_rule( id=del_all_pattern % b.name, expiration=Expiration(days=1),
                        prefix='', status='Enabled',transition=None  )
                b.configure_lifecycle(lf)


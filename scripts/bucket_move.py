import boto

def move( bucket_name ):
    sp = bucket_name.split('-')
    for a in ['an','hd','csvs']:
        if a not in sp:
            return False
    return True

conn = boto.connect_s3( )
new_bucket = conn.get_bucket('an-hdproject-csvs')
for b in conn.get_all_buckets():
    if move( b.name ):
        print b.name
        for k in b.list():
            new_key_name = '%s/%s' % (b.name, k.key)
            print new_key_name
            new_bucket.copy_key( new_key_name, b.name, k.key )
            k.delete()
        b.delete()



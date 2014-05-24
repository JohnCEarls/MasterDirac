import tempfile
import pandas
from pandas.compat import cPickle as pkl, pickle_compat as pc
import pandas.util.testing
import boto
from boto.s3.key import Key
import numpy as np
import logging

  
def sizeof_df(df):   
    dfsize = df.values.nbytes   
    dfsize += df.index.nbytes   
    dfsize += df.columns.nbytes 
    num = df
    for x in ['bytes','KB','MB','GB']:   
        if num < 1024.0 and num > -1024.0:   
            return "%3.1f%s" % (num, x)   
        num /= 1024.0   
    return "%3.1f%s" % (num, 'TB')   

def write_dataframe_to_s3( data_frame, bucket, key ):
    """
    Writes the data_frame (as a pickle) to the given bucket and key
    """
    conn = boto.connect_s3()
    with tempfile.SpooledTemporaryFile() as t:
        try:
            pkl.dump(data_frame, t)#from actual to_pickle code
        except:
            logger = logging.getLogger('write2df')
            logger.exception("DFsize : %s" % sizeof_df( data_frame ))
        t.seek(0)
        k = Key(conn.create_bucket(bucket))
        k.key = key 
        k.set_contents_from_file( t )

def read_dataframe_from_s3( bucket, key, encoding=None ):
    """
    Returns a dataframe from the given bucket and key (where df on s3 is a pickle)
    """
    conn = boto.connect_s3()
    with tempfile.SpooledTemporaryFile() as t:
        k = Key(conn.get_bucket(bucket))
        k.key = key 
        k.get_contents_to_file( t )
        t.seek(0)
        try:
            try:
                pkl.dump(data_frame, t)#from actual to_pickle code
            except:
                logger = logging.getLogger('write2df')
                logger.exception("DFsize : %s" % sizeof_df( data_frame ))
                raise
        except:
            try:
                t.seek(0)
                return pc.load(t, encoding=encoding, compat=False)
            except:
                t.seek(0)
                return pc.load(t, encoding=encoding, compat=True)

if __name__ == "__main__":
    df2 = pandas.DataFrame()
    write_dataframe_to_s3( df2, 'an-scratch-bucket', 'tmp.pkl')
    df3 = read_dataframe_from_s3( 'an-scratch-bucket', 'tmp.pkl' )

    print "(%i,%i)" %  (0,0)
    pandas.util.testing.assert_frame_equal( df2, df3 , check_names = True)

    tests = [(1,1),(10,12),(40000,1000)]
    for a,b in tests:
        print "(%i,%i)" %  (a,b)
        key_name = 'tmp-%i-%i' % (a,b)
        print "testing %s" % key_name
        df2 = pandas.DataFrame(np.random.randn(a, b))
        write_dataframe_to_s3( df2, 'an-scratch-bucket', key_name )
        df3 = read_dataframe_from_s3( 'an-scratch-bucket',key_name )
        pandas.util.testing.assert_frame_equal( df2, df3 , check_names = True)

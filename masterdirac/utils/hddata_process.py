import pandas.io.parsers
import boto
from boto.s3.key import Key
import os
import os.path
import logging
import pandas.io.parsers
from pandas.io import parsers
import numpy as np
import cPickle as pickle
from pandas import DataFrame
from boto.dynamodb2.table import Table
from boto.exception import S3ResponseError
from boto.s3.connection import Location

join = os.path.join

def get_from_s3( source_bucket, data, meta_data, annotation_data, syn_file, agilent_file, data_dir='/scratch/sgeadmin/hddata/', force_write=False ):
    """
    Retrieves data, meta_data and annotation_data from s3 bucket and writes them to the data_dir
    If force_write is False, it only downloads the files if the file does not already exist.
    If force_write is True, it downloads and overwrites the current file.
    returns the local location of data, meta_data, annotation_data
    """
    raise Exception("Deprecated")
    #not sure this is still being used
    #has been removed from servermanager
    #see if i
    logger = logging.getLogger("get_from_s3")
    logger.info("Running: getFromS3('%s','%s','%s','%s','%s',%s)"%( source_bucket, data, meta_data, annotation_data, data_dir, str(force_write) ))
    #check that data dir exists, if not create it
    if not os.path.exists(data_dir):
        logger.info( "Creating directory [%s]" % data_dir )
        os.makedirs(data_dir)

    conn = boto.connect_s3()
    b = conn.get_bucket(source_bucket)
    file_list = [data,meta_data,annotation_data, syn_file, agilent_file]
    local_file_list = []
    for f in file_list:
        s3_path, fname = os.path.split(f)
        local_path = os.path.join(data_dir, fname)
        exists = os.path.exists(local_path)
        local_file_list.append(local_path)
        if exists:
            logger.warning("%s exists"%local_path)
        if force_write or not exists:
            if force_write:
                logger.info('force_writ on')
        try:
            logger.info( "Transferring s3://%s/%s to %s" % (source_bucket,f, local_path ))
            k = Key(b)
            k.key = f #fname
            k.get_contents_to_filename(local_path)
            logger.info("Transfer complete")
        except S3ResponseError as sre:
            logger.error("bucket:[%s] file:[%s] download." % (source_bucket,f))
            logger.error(str(sre))
            raise(sre)
    logger.info("Complete")
    return local_file_list

class HDDataGen:
    """
    Generates a stripped down datatable (pandas data frame) for
    GPUDirac

    working_bucket - str, name of s3 bucket in us-east-1 where the dataframe
                    will be written
    """
    def __init__(self, working_dir ):
        self.logger = logging.getLogger('HDDataGen')
        self.working_dir = working_dir

    def _get_data( self, data_file, annotations_file):
        """
        Given data file and annotations, make dataframe indexed by
        ProbeName with control probes dropped
        """
        data_orig = parsers.read_table( join(self.working_dir,data_file) )
        annot = self._get_annotations( annotations_file)
        #set data index
        data_orig.index = annot['ProbeName']
        return self._drop_controls( data_orig, annotations_file )

    def generate_dataframe(self, source_data, network_config ):
        (data_file, annotations_file, agilent_file, synonym_file, network_table, 
                source_id) =  (
         source_data[ 'data_file'], source_data[ 'annotations_file'],
         source_data[ 'agilent_file'],source_data[ 'synonym_file'],
         network_config[ 'network_table'], network_config[ 'network_source'])
        (data_file, annotations_file, agilent_file, synonym_file) = self.strip_s3_path(
         (data_file, annotations_file, agilent_file, synonym_file) )
        self.logger.debug("Getting base data table")
        data_orig = self._get_data( data_file, annotations_file )
        self.logger.debug("Mapping probes to genes")
        probe_map = self._get_probe_mapping( agilent_file )
        self.logger.debug("Getting genes in given networks")
        network_genes = self._get_network_genes( network_table, source_id )
        self.logger.debug("Adding synonyms")
        syn_map = self._get_synonyms( probe_map, network_genes, synonym_file )
        self.logger.debug("Mapping probes to genes in networks")
        ng2pm = self._get_probe_to_gene_map( probe_map, syn_map )
        self.logger.info("Creating DataFrame")
        new_df = DataFrame(np.zeros( (len(ng2pm), len(data_orig.columns))),
            index=ng2pm.keys(), columns=data_orig.columns )
        #map each network gene to the median of synonymous probes
        self.logger.debug("Aggregating multiple probes to single genes"
                " as median values" )
        for k, probes in ng2pm.iteritems():
            new_df.ix[k] = data_orig.ix[probes].median()
        nm_size = self._estimate_net_map( ng2pm )
        return new_df, nm_size

    def strip_s3_path(self, files):
        """
        Given a full s3_path return the filename
        """
        return tuple([os.path.split( f )[-1] for f in files])
        

    def _get_probe_mapping(self, agilent_file):
        """
        Given an agilent file that maps probe ids to gene symbols
        return dataframe with ProbeID and GeneSymbol columns
        """
        agl = parsers.read_table( join( self.working_dir, agilent_file ) )
        agl.set_index('ProbeID')
        agl2 = agl[agl['GeneSymbol'].notnull()]
        agl2 =  agl2.set_index('ProbeID')
        return agl2

    def _get_synonyms(self, probe_mapping, network_genes, synonym_file):
        #genes in the data file
        base_gene_symbols = set(probe_mapping['GeneSymbol'])
        #genes in the network file
        network_genes = set(network_genes)
        #syn map is a mapping between genes in the base gene symbols set
        #to genes in the network genes set
        def parse_line( line ):
            line_list = []
            parsed = line.split()
            base = parsed[2]
            for part in line.split():
                for part_i in part.split('|'):
                    if len(part_i) > 2:
                        line_list.append(part_i)
            return base, line_list
        syn_map = {}
        with open(join( self.working_dir, synonym_file), 'r') as syn:
            for line in syn:
                base_gene, parsed = parse_line( line )
                for poss_gene in parsed:
                    if poss_gene in network_genes and \
                        base_gene in base_gene_symbols:
                        if base_gene not in syn_map:
                            syn_map[base_gene] = set()
                        syn_map[base_gene].add(poss_gene)
        return syn_map

    def _get_probe_to_gene_map(self, probe_mapping, syn_map ):
        net_gene_to_probes = {}
        for probe_gene, net_genes in syn_map.iteritems():
            for net_gene in net_genes:
                probes = probe_mapping[probe_mapping['GeneSymbol'] == probe_gene ].index
                net_gene_to_probes[net_gene] = probes.tolist()
        return net_gene_to_probes

    def _get_network_genes( self, network_table, source_key, region='us-east-1'):
        """
        Given a dynamodb table and a source_key (network set name)
        return a set of all gene symbols in the network set
        """
        conn = boto.dynamodb.connect_to_region( region )
        table = conn.get_table( network_table )
        network_genes = set()
        self._nets = []
        for item in table.query( source_key ):
            gene_list = item['gene_ids']
            gene_list = gene_list[6:]
            gene_list = gene_list.split('~:~')
            self._nets.append( set(gene_list) )
            for gene in gene_list:
                network_genes.add(gene)
        return network_genes

    def _get_annotations(self, annotations_file):
        return parsers.read_table( join( self.working_dir, annotations_file ) )

    def _drop_controls( self,data, annotation_file):
        annot = self._get_annotations( annotation_file )
        control_probes = annot['ProbeName'][annot['ControlType'] != 0]
        data = data.drop(control_probes)
        return data

    def write_to_s3( self, dataframe,  dest_data,
            location=Location.DEFAULT):
        """
        Writes the dataframe(cleaned and aggregated source data) and the
        metadata file to the given S3 bucket
        """
        dataframe_name = dest_data['dataframe_file']
        bucket_name = dest_data['working_bucket']
        self.logger.info("Sending cleaned dataframe to s3://%s/%s" % (
            bucket_name, dataframe_name) )
        conn = boto.connect_s3()
        bucket = conn.create_bucket(bucket_name, location=location)
        dataframe.to_pickle('temp.tmp')
        for fname in [dataframe_name]:
            k = Key(bucket)
            k.key = dataframe_name
            k.storage_class = 'REDUCED_REDUNDANCY'
            k.set_contents_from_filename('temp.tmp')
        os.remove('temp.tmp')

    def _estimate_net_map( self, ng2pm ):
        gn = global_net_genes = set( ng2pm.keys() )
        nm_size = [len(ln.intersection(gn)) for ln in self._nets]
        return nm_size

if __name__ == "__main__":
    logging.basicConfig(filename='/scratch/sgeadmin/hddata_process.log', level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.info("Starting... hddata_process.py")

    """
    source_bucket = 'hd_source_data'
    d = 'norm.mean.proc.txt'
    md = 'metadata.txt'
    ad = 'annodata.txt'
    agilent_file = 'HDLux_agilent_gene_list.txt'
    syn_file = 'Mus_homo.gene_info'
    network_table = 'net_info_table'
    data_dir='/scratch/sgeadmin/hddata/'

    data, meta_data, anno_data,syn_file,agilent_file = getFromS3( source_bucket,d,md,ad, syn_file, agilent_file, data_dir)
    working_bucket = 'hd_working_0'
    #df_path, df = mapNewData(working_bucket, data, meta_data, anno_data,syn_file,agilent_file, network_table)"""

    #TODO rework this
    #data_file, meta_data, anno_data,syn_file,agilent_file = getFromS3( source_bucket,d,md,ad, syn_file, agilent_file, data_dir)
    source_bucket = 'hd_source_data'
    working_dir = '/scratch/sgeadmin/test'
    data_file = 'norm.mean.proc.txt'
    annotations_file = 'annodata.txt'
    agilent_file = 'HDLux_agilent_gene_list.txt'
    synonym_file = 'Mus_homo.gene_info'
    network_table = 'net_info_table'
    source_id = 'c2.cp.biocarta.v4.0.symbols.gmt'
    bucket_name = 'hd_working_0'

    #generate dataframe
    hddg = HDDataGen( working_dir )
    df = hddg.generate_dataframe( data_file, annotations_file, agilent_file,
         synonym_file, network_table, source_id )
    dataframe_name = 'trimmed_dataframe.pandas'
    hddg.write_to_s3( bucket_name, df,  dataframe_name)
    #copy metadata
    conn = boto.connect_s3()
    bucket = conn.create_bucket(source_bucket, location=Location.DEFAULT)
    k = Key(bucket)
    k.key = 'metadata.txt'
    k.copy(bucket_name, k.name)

    logging.info("Ending... hddata_process.py")

import masterdirac.utils.hddata_process as hd
import logging
import os.path

if __name__ == "__main__":
    logging.basicConfig( level=logging.DEBUG)
    logging.info("Test data")
    sd = source_data = {}
    network_config = {}
    source_bucket = sd['source_bucket']  ='aurea-nebula'
    data = sd['data_file'] ='hd-source-data/exp_mat_b6_wt_q111.txt'
    meta_data = 'hd-source-data/metadata_b6_wt_q111.txt'
    annotation_data = sd['annotations_file'] = 'hd-source-data/annodata_b6.txt'
    syn_file = sd['synonym_file'] = 'hd-source-data/Mus_homo.gene_info'
    agilent_file = sd['agilent_file'] = 'hd-source-data/HDLux_agilent_gene_list.txt'
    data_dir = os.path.dirname(os.path.realpath(__file__))
    force_write = True
    print data_dir
    #hd.get_from_s3( source_bucket, data, meta_data, annotation_data, syn_file, 
    #        agilent_file, data_dir, force_write)
    network_config['network_table'] = 'net_info_table'
    network_config['network_source'] = 'c2.cp.biocarta.v4.0.symbols.gmt'
    hddg =  hd.HDDataGen( data_dir )
    new_df, nm_size = hddg.generate_dataframe( source_data, network_config )
    new_df.save('created_dataframe.pandas')


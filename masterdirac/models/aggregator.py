
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute,UnicodeSetAttribute

class TruthGPUDiracModel(Model):
    class Meta:
        table_name='truth_gpudirac_hd'
    run_id = UnicodeAttribute(hash_key=True)
    strain_id = UnicodeAttribute(range_key=True)
    accuracy_file = UnicodeAttribute(default='')
    bucket =  UnicodeAttribute(default='')
    result_files =  UnicodeAttribute(default='')
    timestamp =  UnicodeAttribute(default='')
    pval_file = UnicodeAttribute(default='')
    mask = UnicodeSetAttribute(default=[])

class RunGPUDiracModel(Model):
    class Meta:
        table_name='run_gpudirac_hd'
    run_id = UnicodeAttribute(hash_key=True)
    timestamp = UnicodeAttribute(range_key=True)
    config = UnicodeAttribute(default='')
    k = UnicodeAttribute(default='')

class DataForDisplay(Model):
    class Meta:
        table_name = 'tcdirac-data-for-display'
    identifier = UnicodeAttribute(hash_key = True)
    timestamp = UnicodeAttribute(range_key=True)
    strains = UnicodeSetAttribute(default=[])
    alleles = UnicodeSetAttribute(default=[])
    description = UnicodeAttribute(default='')
    data_bucket = UnicodeAttribute(default='')
    data_file = UnicodeAttribute(default='')
    qvalue_file = UnicodeAttribute(default='')
    column_label = UnicodeAttribute(default='')
    row_label = UnicodeAttribute(default='')
    network = UnicodeAttribute(default='')

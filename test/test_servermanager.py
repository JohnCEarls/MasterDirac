from nose import with_setup # optional
import logging
from masterdirac.controller.servermanager import ServerManager
import masterdirac.models.master as master_mdl 



class TestSM:
    def setUp(self):
        logging.getLogger("boto").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("pynamodb").setLevel(logging.WARNING)

        self.master_name = 'testmanager'
        self.sm = ServerManager(self.master_name)

    def tearDown(self):
        self.cleanup_master_model()
        self.sm = None

    def cleanup_master_model(self):
        master_record = master_mdl.ANMaster.get( self.sm._master_name )
        master_record.delete()


    def test_master_model_not_None(self):
        assert( self.sm._master_model is not None )

    def test_master_model_created(self):
        mm = master_mdl.get_master(self.master_name)
        for k,v in mm.iteritems():
            assert self.sm._master_model[k] == v




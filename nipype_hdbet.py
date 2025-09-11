#! /usr/bin/env python 



from nipype.interfaces.base.core import BaseInterface, BaseInterfaceInputSpec
from nipype.interfaces.base import traits, TraitedSpec
from HD_BET.checkpoint_download import maybe_download_parameters
from HD_BET.hd_bet_prediction import get_hdbet_predictor, hdbet_predict
from os.path import basename, abspath

""" 
inputs:

    in_file 
    output
    disable_tta






run:

    maybe_download_parameters()
    predictor = get_hdbet_predictor(
            ) 
            
"""

class HD_BET_BrainextractorInputSpec(BaseInterfaceInputSpec):
    in_file = traits.File(exists=True, mandatory=True)

class HD_BET_BrainextractorOutputSpec(TraitedSpec):
    out_file = traits.File(exists=True, mandatory=True)
    out_mask = traits.File(exists=True, mandatory=True)

class HD_BET_Brainextractor(BaseInterface):
    input_spec = HD_BET_BrainextractorInputSpec
    output_spec = HD_BET_BrainextractorOutputSpec


    def _run_interface(self, runtime):

        maybe_download_parameters()
        predictor = get_hdbet_predictor(
            )

        output_filename =basename(self.inputs.in_file)
        hdbet_predict(self.inputs.in_file, output_filename, predictor, keep_brain_mask=True)


       # self._results['out_file'] = abspath(output_filename)
        

        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        output_filename =basename(self.inputs.in_file)
        mask_filename = output_filename.replace('.nii', '_bet.nii')
        
        outputs['out_file'] = abspath(output_filename)
        outputs['out_mask'] = abspath(mask_filename) 
        return outputs  




def test_hdbet(in_file):

    from nipype import Node
    hdbet = Node(HD_BET_Brainextractor(), name='ndbet')
    hdbet.inputs.in_file = in_file
    results = hdbet.run()

    print(results)

    return results

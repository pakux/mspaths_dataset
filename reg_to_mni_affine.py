#! env python3
# -*- coding: utf-8 -*-

__author__ = "Paul Kuntke"
__email__ = "paul.kuntke@uniklinikum-dresden.de"
__license__ = "BSD 3 Clause"

__derivative__ = "reg_to_mni_affine"
__version__ = "0.2"
__longitudinal__ = False

from os.path import abspath
from nipype import IdentityInterface, MapNode, Node, Workflow
from nipype.interfaces.utility import Rename, Split, Merge
from nipype.interfaces.fsl import Info

from nipype.interfaces.ants import (
    RegistrationSynQuick,
    BrainExtraction,
    ApplyTransforms,
)
from nipype.interfaces.fsl.maths import ApplyMask

from wmi_nipype_workflows.register_to_mni import coregister_to_mni_wf

from wmi_nipype_workflows.reports import RegistrationRPT, SegmentationRPT
from wmi_nipype_workflows.wmi_workflow import WmiWorkflow

from nipype_hdbet import HD_BET_Brainextractor
from nilearn.datasets import load_mni152_template

"""
This workflow registers Images into MNI-Space

"""

inputqueries = {
    "T1w": dict(
        suffix="T1w",
        space=None,
        extension="nii.gz",
        run=None,
        ceagent=None,
        acquisition=None,
        scope="raw",
    ),
    "FLAIR": dict(
        space=None,
        suffix="FLAIR",
        extension="nii.gz",
        run=None,
        ceagent=None,
        acquisition=None,
        scope="raw",
    ),
    "xfm": {
        "from": "T1w",
        "to": "MNI152",
        "desc": "forward",
        "suffix": "xfm",
        "extension": "mat",
    },
}


@WmiWorkflow
def gen_wf(available_inputs=[], report_dir="/home/paulkuntke/mspaths/report"):
    """
    Create actual WmiWorkflow
    """

    outputfields = [
        "anat_@t1w",
    ]
    outputnode = Node(IdentityInterface(fields=outputfields), name="outputnode")
    t1w_split = Node(Split(splits=[1], squeeze=True), name="t1w_split")  # Squeeze T1w
    hdbet = Node( HD_BET_Brainextractor(), name="hdbet")
    template = Info.standard_image("MNI152_T1_1mm.nii.gz")

    applyMatrix = Node(ApplyTransforms(), name="applyMatrix")
    applyMatrix.inputs.reference_image = template

    rename_t1w = Node(
        Rename(
            format_string="sub-%(subject)s_ses-%(session)s_space-MNI152_T1w",
            keep_ext=True,
        ),
        name="rename_t1w",
    )

    gen_wf.wf = Workflow(name=__derivative__)
    gen_wf.wf.connect(
        [
            (
                gen_wf.inputnode,
                rename_t1w,
                [("subject", "subject"), ("session", "session")],
            ),
            (
                gen_wf.inputnode,
                applyMatrix,
                [("xfm", "transforms")],
            ),
            (gen_wf.inputnode, t1w_split, [("T1w", "inlist")]),
            (t1w_split, applyMatrix, [("out1", "input_image")]),
            (applyMatrix, hdbet, [("output_image", "in_file")]),
            (hdbet, rename_t1w, [("out_file", "in_file")]),
            (rename_t1w, outputnode, [("out_file", "anat_@t1w")]),

        ]
    )

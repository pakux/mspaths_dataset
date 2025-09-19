#! /usr/bin/env python

# reasd 

import pandas as pd



def get_session_list(seubject_id, bidsroot):
    

def get_session_date(subject_id, session_id, bidsroot):

    if type(session_id) is int:
        session_id = str(session_id)

    if type(session_id) is str:
        session_id = session_id.str.replace('ses-', '')
        session_id = [session_id]

    if session_id is None:
        get_session_list(subject_id)
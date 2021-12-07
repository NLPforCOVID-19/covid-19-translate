import os
import copy
from pyknp import Juman
from unicodedata import normalize
import re
import json
import sys
import time

parent_dir = '/mnt/hinoki/share/covid19/run/topic_classification_log'
#input_file = os.path.join(parent_dir, "output_10lines.jsonl")
input_file = os.path.join(parent_dir, "output.jsonl")

def get_input(input_file):
    lines = open(input_file, 'r', errors='ignore').readlines()
    return lines 

input_lines = get_input(input_file)
for i, input_line in enumerate(input_lines):
    meta = json.loads(input_line.strip())
    title = meta['ja_translated']['title']
    file_name = meta['orig']['file']
    link = os.path.join('/mnt/hinoki/share/covid19/', file_name)
    print (f'title: {title}\npath_to_file: {link}\n')

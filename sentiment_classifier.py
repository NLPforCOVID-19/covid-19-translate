import gc
import os
import copy
from pyknp import Juman
from unicodedata import normalize
import re
import json
import sys
from argparse import ArgumentParser
from pathlib import Path
import time

import torch
import torch.nn as nn
from pyknp import Juman
from transformers import BertTokenizer

from custom_model import BertForSequenceRegression
from utils import Hook, set_seed, get_basename, set_device


parent_dir = '/mnt/hinoki/share/covid19/run/topic_classification_log'
input_file = os.path.join(parent_dir, "output.jsonl")
output_file = os.path.join(parent_dir, "output_with_sent.jsonl")
accessed_list = os.path.join(parent_dir, "sent_list.txt") 

def title_clean(title_ls):
    tmp_ls = copy.deepcopy([title_ls])
    for i in range(len(tmp_ls) - 1):
        if tmp_ls[i] is None:
            del tmp_ls[i]
    for i in range(len(tmp_ls)):        
        tmp_ls[i] = normalize('NFKC', tmp_ls[i])
        tmp_ls[i] = tmp_ls[i].replace(' ', '')
        tmp_ls[i] = re.sub(r'−.+?$', '', tmp_ls[i])
        tmp_ls[i] = re.sub(r'ーY.+?$', '', tmp_ls[i])
        tmp_ls[i] = re.sub(r'\|.+?$', '', tmp_ls[i])
    jumanpp = Juman()
    sep_ls = []
    for tmp in tmp_ls: 
        sep_ls.append(' '.join([mrph.midasi for mrph in jumanpp.analysis(tmp)]))
    return sep_ls[0]

def extract_meta_add_sentiment(meta, model, jumanpp, tokenizer, device):
    title = meta['ja_translated']['title']
    model.eval()
    with torch.no_grad():
        title = title_clean(title)
        try:
            input_sentence = title
            split = ' '.join([mrph.midasi for mrph in jumanpp.analysis(input_sentence).mrph_list()])
            inputs = tokenizer(split,
                               max_length=config.params.max_seq_len,
                               padding='max_length',
                               truncation=True,
                               return_tensors='pt')

            for key, value in inputs.items():
                inputs[key] = value.to(device)

            output = model(inputs)
            score = output.squeeze().item()  # (1, )
            print(f'{split}: {score:.3f}')
        except Exception as e:
            cls, _, tb = sys.exc_info()
            score = -1 
            print(f'{cls.__name__}: {e.with_traceback(tb)}')
    meta["sentiment"] = score

def get_input(input_file):
    lines = open(input_file, 'r', errors='ignore').readlines()
    return lines 

def get_processed_dict(accessed_list):
    classified_dict = {} 
    lines = open(accessed_list, 'r').readlines()
    for line in lines: 
        try:
            res = line.strip().split()[0]
            classified_dict[res] = 1
        except:
            continue
    return classified_dict

def save_sentiment_meta(meta, output_file):
    output_res = json.dumps(meta, ensure_ascii=False)
    with open(output_file, "a+") as f:
        f.write(output_res.strip() + '\n')

def read_output_file(file_path):
    lines = open(file_path, 'r').readlines()
    for line in lines:
        meta = json.loads(line.strip())
        extract_meta_add_sentiment(meta, model, jumanpp, tokenizer, device)
        save_sentiment_meta(meta, output_file)


def check_unprocessed(meta, processed_dict):
    link = os.path.join('/mnt/hinoki/share/covid19/', meta['orig']['file'])
    return (not processed_dict.get(link, 0))

def save_result(output_file, accessed_list, meta):
    with open(output_file, "a+") as f:
        output_res = json.dumps(meta, ensure_ascii=False)
        f.write(output_res.strip() + '\n')
    link = os.path.join('/mnt/hinoki/share/covid19/', meta['orig']['file'])
    with open(accessed_list, "a+") as f:
        f.write(link.strip() + '\n')

def save_result_fail(accessed_list, meta):
    link = os.path.join('/mnt/hinoki/share/covid19/', meta['orig']['file'])
    with open(accessed_list, "a+") as f:
        f.write(link.strip() + '\n')


gpu_num2 = "0"
config_path = '/mnt/berry/home/song/covid19-topic-classifier/sentiment/config/ACP.json'
with open(config_path, 'r') as f:
    config = json.load(f, object_hook=Hook)
load_dir = Path(config.path.fine_tuned).joinpath(get_basename(config))
set_seed(0)
device, parallel = set_device(gpu_num2)
tokenizer = BertTokenizer.from_pretrained(load_dir, do_lower_case=False, do_basic_tokenize=False)
model = BertForSequenceRegression.from_pretrained(load_dir).to(device)
if parallel:
    model = nn.DataParallel(model, sent_device_ids=list(map(int, gpu_num2.split(','))))
jumanpp = Juman()

processed_num = {}
while (1):
    print ("Sentiment Classification start")
    processed_dict = get_processed_dict(accessed_list)
    input_lines = get_input(input_file)
    tot_line = len(input_lines)
    for i, input_line in enumerate(input_lines):
        if (processed_num.get(i, 0) == 1):
            continue
        processed_num[i] = 1
        meta = json.loads(input_line.strip())
        link = os.path.join('/mnt/hinoki/share/covid19/', meta['orig']['file'])
        if (check_unprocessed(meta, processed_dict)):
            extract_meta_add_sentiment(meta, model, jumanpp, tokenizer, device)
            print (f'Sentiment: {i} of {tot_line}')
            print (meta['orig']['file'])
            print (meta['sentiment'])
            save_result(output_file, accessed_list, meta)
    time.sleep(1000)
    del processed_dict
    del input_lines
    gc.collect()

#read_processed_name()
#read_output_file()
#process_and_save()
#titles = ['新型コロナ　茨城で新たに9人感染、1人死亡　新規感染者1桁は6月21日以来104日ぶり　県と水戸市発表', 'アラスカの新型コロナ危機で医師は誰が生きて誰が死ぬかを決定しなければ', '改革財源への資金提供オーストラリア政府保健省']
#classify_titles(titles)


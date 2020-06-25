from typing import List
import os
import sys
import re
import time
import json
import glob
import random
from pathlib import Path

import requests as req
from requests_oauthlib import OAuth1


source_langs = ["cn", "es", "eu", "us", "int", 'in', "kr", "jp", "de", "fr", "en", "zh", "ko"]

NAME =
KEY=
SECRET=

consumer = OAuth1(KEY, SECRET)
En_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_en_ja/'
Zh_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_zh-CN_ja/'
Ko_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/patentN_ko_ja/'
Fr_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalN_fr_ja/'
Es_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalN_es_ja/'
De_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalN_de_ja/'
URLs = {"en":En_Ja_URL, "eu":En_Ja_URL, "us":En_Ja_URL, "int":En_Ja_URL, "zh":Zh_Ja_URL, "cn":Zh_Ja_URL, "kr":Ko_Ja_URL, "ko": Ko_Ja_URL, "fr":Fr_Ja_URL, "es": Es_Ja_URL, "de": De_Ja_URL, 'in': En_Ja_URL}


def translator_part(input_part: str, lang:str) -> str:
    if (lang == 'jp' or lang== 'ja'):
        return input_part
    if (lang not in source_langs):
        return ''
    global KEY, NAME, SECRET, consumer
    URL=URLs[lang]

    params = {
        'key': KEY,
        'name': NAME,
        'type': 'json', # response type: ['xml', 'json']
        'split': 1, # 1: cut the documents into sentences automatically
        'text': input_part
    }

    try:
        res = req.post(URL, data=params, auth=consumer)
        res.encoding = 'utf-8'
        result = json.loads(res.text)
        result = result['resultset']
        #original_sentence = result['request']['text']
        translated_sentence = result['result']['text']
        return translated_sentence
    except Exception as e:
        print ("Error from Minhon:")
        print('type:' + str(type(e)))
        print('args:' + str(e.args))
        print('e:' + str(e))
        print (lang)
        return ''

def translate_line(text: str, lang: str) -> str:
    return "test"

def process_file(file_name: str) -> None:
    output_file = str(Path(file_name).with_suffix(".translated"))

    with open(file_name, "r") as f:
        lines = f.readlines()

    with open(output_file, "w") as f:
        for i, tline in enumerate(lines):
            json_line = json.loads(tline)
            text = json_line['text']
            lang = json_line['lang']
            translated_text = translator_part(text, lang)
            json_line['translated_text'] = translated_text.strip()
            print ("Original line: {}\n, Translated line: {}\n".format(text, json_line['translated_text']))
            tline = json.dumps(json_line)
            f.write(tline.strip()+'\n')

def extract_text(input_file, output_file):
    with open(input_file, "r") as f:
        lines = f.readlines()

    #for i, tline in enumerate(lines):
    #    json_line = json.loads(tline)
    #    lang = json_line['lang']
    #    ori_text = json_line['text']
    #    text = json_line['translated_text']
    #    if (lang=='en'):
    #        print (ori_text)

    with open(output_file, "w") as f:
        for i, tline in enumerate(lines):
            json_line = json.loads(tline)
            lang = json_line['lang']
            text = json_line['translated_text']
            if (text!=''):
                f.write(text.strip()+'\n')


file_name = '/mnt/hinoki/share/covid19/twitter/data/2020-06-03-08-00-00'
process_file(file_name)
extract_text('/mnt/hinoki/share/covid19/twitter/data/2020-06-03-08-00-00.translated','/mnt/hinoki/share/covid19/twitter/data/2020-06-03-08-00-00.translated_text')

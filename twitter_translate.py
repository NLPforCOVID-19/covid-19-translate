from typing import List
import os
import sys
import re
import time
import json
import glob
import random
from datetime import datetime, timedelta
from pathlib import Path
import requests as req
from requests_oauthlib import OAuth1

from bs4 import BeautifulSoup

from xml.etree.ElementTree import *
from elastic_search_utils import ElasticSearchTwitterImporter

tolang = sys.argv[1]
account_index = int(sys.argv[2])

parent_folder = '/mnt/hinoki/share/covid19/twitter/html'
tweet_folder = "/mnt/hinoki/share/covid19/run/new-html-files"
extract_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/extract_tweet_list.txt'
en_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/en_tweet_trans_list.txt'
ja_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/ja_tweet_trans_list.txt'
log_folder = '/mnt/hinoki/share/covid19/run/new-translated-files'

source_langs = ["cn", "es", "eu", "us", "int", 'in', "kr", "jp", "de", "fr", "en", "zh", "ko"]

account_list = [ (NAME1, KEY1, SECRET1), (NAME2, KEY2, SECRET2)]

NAME, KEY, SECRET = account_list[account_index]

consumer = OAuth1(KEY, SECRET)
# anylang -> Ja
En_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_en_ja/'
Zh_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_zh-CN_ja/'
Ko_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ko_ja/'
Fr_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_fr_ja/'
Es_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_es_ja/'
De_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_de_ja/'
Pt_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_pt_ja/'
Id_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_id_ja/'

# anylang -> En
Ja_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ja_en/'
Zh_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_zh-CN_en/'
Ko_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ko_en/'
Fr_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_fr_en/'
Es_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_es_en/'
De_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_de_en/'
Pt_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_pt_en/'
Id_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_id_en/'

# anylang -> Ja
# Ja -> is not needed
Ja_URLs = {'np':En_Ja_URL, 'za': En_Ja_URL, "au":En_Ja_URL, "br":Pt_Ja_URL, "en":En_Ja_URL, "eu":En_Ja_URL, "sg": En_Ja_URL, "us":En_Ja_URL, "int":En_Ja_URL, "zh":Zh_Ja_URL, "cn":Zh_Ja_URL, "kr":Ko_Ja_URL, "fr":Fr_Ja_URL, "es": Es_Ja_URL, "de": De_Ja_URL, 'in': En_Ja_URL, 'my':Id_Ja_URL}

# anylang -> En
# np, za, au, en, eu, us, int, in is not needed
En_URLs = {"ja": Ja_En_URL, "jp": Ja_En_URL, "br":Pt_En_URL, "cn":Zh_En_URL, "kr":Ko_En_URL, "fr":Fr_En_URL, "es": Es_En_URL, "de": De_En_URL, 'my':Id_En_URL}

if (tolang == 'ja'):
    accessed_file_list = ja_accessed_file_list
    URLs = Ja_URLs
    log_file_base_lang = "new-twitter-translated-files"
elif (tolang == 'en'):
    accessed_file_list = en_accessed_file_list
    URLs = En_URLs
    log_file_base_lang = "new-twitter-translated-files-{}".format(tolang)
loc_phrase = "{}_translated".format(tolang)

# elastic search
es_host = 'basil505'
es_port = 9200
es_ja_index = 'covid19-tweets-ja'
es_en_index = 'covid19-tweets-en'
es_index = ''
html_dir = "/mnt/hinoki/share/covid19/twitter/html"
es_ja_importer = ElasticSearchTwitterImporter(es_host, es_port, html_dir, 'ja', logger=None)
es_en_importer = ElasticSearchTwitterImporter(es_host, es_port, html_dir, 'en', logger=None)
if (tolang == 'ja'):
    es_importer = es_ja_importer
    es_index = es_ja_index
elif (tolang == 'en'):
    es_importer = es_en_importer
    es_index = es_en_index

def translator_part(input_part: str, lang:str) -> str:
    if (lang==tolang):
        return input_part

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
    except:
        return ""
    result = result['resultset']
    #original_sentence = result['request']['text']
    translated_sentence = result['result']['text']
    return translated_sentence

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

def get_text_from_html(input_html):
    with open(input_html) as f:
        soup = BeautifulSoup(f, 'html.parser')
    first_p = soup.find('p')
    if (first_p != None):
        return (first_p.text)
    else:
        return ""
    

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

def get_extracted_files(filename):
    names = {}
    with open(filename, "r") as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            names[line] = 1
    return names

def get_translated_files(filename):
    names = {}
    with open(filename, "r") as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            try:
                name, status = line.split()
            except:
                continue
            names[name] = 1
    return names

def write_to_accessed_line(orig_file, status):
    with open(accessed_file_list, "a+") as f:
        line = "{} {}".format(orig_file, status)
        f.write(line.strip()+'\n')

def text_to_html(text):
    lines = text.split('\n')
    title = '<head><meta charset="utf-8"/></head>'
    content = lines
    output_html = "<html>\n{}\n<body>\n".format(title)
    for line in content:
        line = "<p>{}</p>\n".format(line)
        output_html += line
    output_html += "</body>\n</html>\n"
    return output_html

def save_result(output_text, translated_txt):
    translated_html = str(Path(translated_txt).with_suffix(".html"))

    status = 0

    with open(translated_txt, "w") as f:
        f.write(output_text)

    output_html = text_to_html(output_text)
    with open(translated_html, "w") as f:
        f.write(output_html)

    #elastic search
    try:
        res = es_importer.update_record(translated_txt, index=es_index, is_data_stream=True)
    except:
        print ("Twitter translation Error: elastic")
        status = 3
    return status


year_month_day_hour_pattern = re.compile(".*?/(\d\d\d\d)/(\d\d)/(\d\d)-(\d\d).*")
def my_get_time():
    dt = datetime.now()
    year, month, day, hour, minute = dt.year, dt.month, dt.day, dt.hour, dt.minute
    return year, month, day, hour, minute


pattern = re.compile('^(\n*).*?(\n*)$')
def get_start_end_newlines(text):
    m = re.search(pattern, text)
    if (m!=None):
        return m.group(1), m.group(2)
    else:
        return "", ""

def save_hashtag_url(text):
    text = re.sub("(https://(.*/)+.*? )", " ｟\g<1> ｠ ", text) # Remove urls from text.
    text = re.sub("([#＃]\w+\s*)", " ｟\g<1> ｠ ", text) # Remove hashtags from text.
    return text

#http_p = re.compile("([(?:https)|(?:http)]://(?:.*/)+.*? )")
#hashtag_p = re.compile("[#＃]\w+\s*")
#def add_space_parts(text):
#    res = []
#    http_g = re.findall(http_p, text)
#    if (http_g != None):
#        print (1)
#        print (http_g)
#        print (2)
#        #for g in http_g:
#        #    print (g.group())

#def add_space_hashtag_url(text):
#    text = re.sub("(https://(.*/)+.*? )", " \g<1> ", text) # Remove urls from text.
#    text = re.sub("([#＃]\w+\s*)", " \g<1> ", text) # Remove hashtags from text.
#    return text


def translate_file(name):
    #'/mnt/hinoki/share/covid19/twitter/html/us/orig/2020/12/15/11-35/1338674046616162305.html'

    orig_html = name
    orig_json = str(Path(orig_html).with_suffix(".json"))
    country = orig_json.replace(parent_folder, "").split('/')[0] 
    translated_html = orig_html.replace("/orig/", "/{}_translated/".format(tolang)) 
    translated_txt = str(Path(translated_html).with_suffix(".txt")) # replace with translated folder
    translated_folder = os.path.dirname(translated_html)
    try:
        os.makedirs(translated_folder, exist_ok=True)
    except:
        status = 1
        write_to_accessed_line(orig_html, country, status)
        return

    line = open(orig_json, "r").readline()
    json_line = json.loads(line.strip())

    lang = json_line['lang']
    #text = json_line['text']
    text = get_text_from_html(orig_html)
    if (text == ""):
        status = 1
        write_to_accessed_line(orig_html, country, status)
        return

    text = save_hashtag_url(text)
    start_newlines, end_newlines = get_start_end_newlines(text)
    # FIXME
    translated_text = translator_part(text, lang)
    translated_text = start_newlines + translated_text + end_newlines
    print ("Begin")
    print (text)
    print (translated_text)
    status = save_result(translated_text, translated_txt)
    print (status)

    #orig_text =text

    write_to_accessed_line(orig_html, country, status)
    year, month, day, hour, minute = my_get_time()
    minute = 0
    log_file_base = "{}-{:04d}-{:02d}-{:02d}-{:02d}-{:02d}.txt".format(log_file_base_lang, year, month, day, hour, minute)
    log_file  = "{}/{}".format(log_folder, log_file_base)
    with open(log_file, "a+") as f:
        f.write(translated_html.strip()+'\n')
    print ("Output: {}".format(translated_html))

#text = "this is a test http://dsckl.html https://t.co/thisis/123lksdjf/skd.html test2 #hashtag#hastag #daf this is another test"
#orig_text =text
#parts = add_space_parts(text)
#print (parts)
#exit()
#text = save_hashtag_url(text)
#lang = 'us'
#tolang='ja'
#translated_text = translator_part(text, 'us')
#print ("Original text:",orig_text)
#print ("Added special tokens:", text)
#print ("Translated:", translated_text)
#exit()
itr = 0
while (1):
    extracted_files = get_extracted_files(extract_accessed_file_list)
    translated_files = get_translated_files(accessed_file_list)
    under_translated_files = [name for (name, _) in extracted_files.items() if translated_files.get(name, 0)==0]
    under_translated_files = under_translated_files[-1000:]
    if (len(under_translated_files)==0):
        time.sleep(10)
        continue
    year, month, day, hour, minute = my_get_time()
    print ("Date: {}/{} {}:{}".format(month, day, hour, minute))
    print ("The {}th iteration of lang {}:".format(itr, tolang))
    print ("Number of under_translated files: {}".format(len(under_translated_files)))
    for name in under_translated_files:
        print (name)
        #translate_file(name)
        try:
            translate_file(name)
        except:
            print ("translate file whole error", name)
            write_to_accessed_line(name, 1)
            continue

    time.sleep(10)

#file_name = '/mnt/hinoki/share/covid19/twitter/data/2020-06-03-08-00-00'
#process_file(file_name)
#extract_text('/mnt/hinoki/share/covid19/twitter/data/2020-06-03-08-00-00.translated','/mnt/hinoki/share/covid19/twitter/data/2020-06-03-08-00-00.translated_text')

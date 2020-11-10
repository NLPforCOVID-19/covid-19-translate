# usage here: https://github.com/Alir3z4/html2text/blob/master/docs/usage.md
from typing import List, Tuple
import os
import sys
import re
import time
import json
import glob
import random
from datetime import datetime, timedelta
from pathlib import Path

from bs4 import BeautifulSoup
import html2text

import requests as req
from requests_oauthlib import OAuth1
from xml.etree.ElementTree import *

from elastic_search_utils import ElasticSearchImporter


# input arguments
account_index = int(sys.argv[1])
tolang = sys.argv[2]

# constant
source_langs = ["jp", "cn", "br", "kr", "es", "eu", "us", "int", 'in', "de", "fr", "au", 'za']
map_to_lang = {"jp":"ja", "en":"en", "eu":"en", "us":"en", "int":"en", "in":"en", "au":"en", 'za':'en'}
source_langs_ori = source_langs
parent_folder = '/mnt/hinoki/share/covid19/html'
log_folder = '/mnt/hinoki/share/covid19/run/new-translated-files'
en_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/en_trans_list.txt' 
ja_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/trans_list.txt' 

accessed_file_list_tmp = '/mnt/hinoki/share/covid19/run/trans_log_song/trans_list_tmp.txt' 
tmp_list = '/mnt/hinoki/share/covid19/run/trans_log_song/extract_list.txt' 
log_file_base_lang = ''
elastic_log = '/mnt/hinoki/share/covid19/run/trans_log_song/elastic_log.txt'


# translation settings

account_list = [ (NAME1, KEY1, SECRET1), (NAME2, KEY2, SECRET2), (NAME3, KEY3, SECRET3), (NAME4, KEY4, SECRET4), (NAME5, KEY5, SECRET5), (NAME6, KEY6, SECRET6)]
account_len = len(account_list)

NAME, KEY, SECRET = account_list[account_index] 
#source_langs = [source_langs[account_index]]
if (account_index == 0):
    source_langs = source_langs[0:3]
elif (account_index == 1):
    source_langs = source_langs[3:9]
elif (account_index == 2):
    source_langs = source_langs[9:-2]
elif (account_index == 3):
    source_langs = source_langs[-2:]
elif (account_index == 4):
    source_langs = source_langs[0:6]
elif (account_index == 5):
    source_langs = source_langs[6:]
print (source_langs)

consumer = OAuth1(KEY, SECRET)

# anylang -> Ja
En_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_en_ja/'
#Zh_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_zh-CN_ja/'
Zh_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_zh-CN_ja/'
Ko_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ko_ja/'
Fr_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_fr_ja/'
Es_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_es_ja/'
De_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_de_ja/'
Pt_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_pt_ja/'

# anylang -> En
Ja_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ja_en/'
Zh_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_zh-CN_en/'
Ko_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ko_en/'
Fr_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_fr_en/'
Es_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_es_en/'
De_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_de_en/'
Pt_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_pt_en/'

# anylang -> Ja
# Ja -> is not needed
Ja_URLs = {'za': En_Ja_URL, "au":En_Ja_URL, "br":Pt_Ja_URL, "en":En_Ja_URL, "eu":En_Ja_URL, "us":En_Ja_URL, "int":En_Ja_URL, "zh":Zh_Ja_URL, "cn":Zh_Ja_URL, "kr":Ko_Ja_URL, "fr":Fr_Ja_URL, "es": Es_Ja_URL, "de": De_Ja_URL, 'in': En_Ja_URL}

# anylang -> En
# za, au, en, eu, us, int, in is not needed
En_URLs = {"jp": Ja_En_URL, "br":Pt_En_URL, "cn":Zh_En_URL, "kr":Ko_En_URL, "fr":Fr_En_URL, "es": Es_En_URL, "de": De_En_URL}

# elastic search
#es_host = 'basil201'
es_host = 'basil501'
es_port = 9200
es_ja_index = 'covid19-pages-ja'
es_en_index = 'covid19-pages-en'
es_ja_doc_index = 'covid19-docs-ja'
es_en_doc_index = 'covid19-docs-en'
es_index = ''
es_doc_index = ''
html_dir = "/mnt/hinoki/share/covid19/html"
test_file = '/mnt/hinoki/share/covid19/html/jp/ja_translated/hazard.yahoo.co.jp/c-emg.yahoo.co.jp/notebook/contents/pickup/coronaevac.html/2020/08/26-03-11/coronaevac.txt'
es_ja_importer = ElasticSearchImporter(es_host, es_port, html_dir, 'ja', logger=None)
es_en_importer = ElasticSearchImporter(es_host, es_port, html_dir, 'en', logger=None)
if (tolang == 'ja'):
    es_importer = es_ja_importer
    es_index = es_ja_index
    es_doc_index = es_ja_doc_index
elif (tolang == 'en'):
    es_importer = es_en_importer
    es_index = es_en_index
    es_doc_index = es_en_doc_index

# utilities
year_month_day_hour_pattern = re.compile(".*?/(\d\d\d\d)/(\d\d)/(\d\d)-(\d\d).*")
def my_get_time():
    dt = datetime.now()
    year, month, day, hour, minute = dt.year, dt.month, dt.day, dt.hour, dt.minute
    return year, month, day, hour, minute

def time_priority(name: Tuple[str, str]) -> int:
    name = name[0]
    res = re.match(year_month_day_hour_pattern, name)
    if (res==None):
        print (name)
    assert(res!=None)
    w=100000000
    priority = 0
    for i in range(1,5):
        priority += -1*int(res.group(i))*w
        w/=100
    return priority

def text_to_html(text):
    lines = text.split('\n')
    title = lines[0]
    content = lines[1:]
    output_html = "<html>\n<head>\n<title>{}</title>\n</head>\n<body>\n".format(title)
    for line in content:
        line = "<p>{}</p>\n".format(line)
        output_html += line
    output_html += "</body>\n</html>\n"
    return output_html

def get_lang(input_path):
    global parent_folder
    for lang in source_langs_ori:
        detect_part = "{}/{}/".format(parent_folder, lang)
        if (detect_part in input_path):
            return lang
    return None

def cut_parts(input_text) -> Tuple[List[List[str]], int]:
    """
        cut input_text into shorter parts
    """
    lines = input_text.split('\n')
    line_num = len(lines)
    parts = []
    part = ""
    for line in lines:
        line += '\n'
        if (len(part)+len(line)<300):
            part+=line
        else:
            part = part.strip()
            parts.append(part)
            part = line
    if (len(part)>0):
        part = part.strip()
        parts.append(part)
    return parts, line_num

def tmp2source(tmp_name):
    source_name = tmp_name.replace("/tmp/","/orig/",1)
    source_name = str(Path(source_name).with_suffix(".html"))
    return source_name
    
def source2tmp(tmp_name):
    source_name = tmp_name.replace("/orig/","/tmp/",1)
    source_name = str(Path(source_name).with_suffix(".html"))
    return source_name

def source2target(tmp_name):
    source_name = tmp_name.replace("/orig/","/{}/".format(loc_phrase),1)
    source_name = str(Path(source_name).with_suffix(".html"))
    return source_name

def tmp2target(tmp_name):
    target_name = tmp_name.replace("/tmp/","/{}/".format(loc_phrase),1)
    target_name = str(Path(target_name).with_suffix(".txt"))
    return target_name

def ja_translate2source(name):
    target_name = name.replace("/{}/".format(loc_phrase),"/orig/",1)
    target_name = str(Path(target_name).with_suffix(".html"))
    return target_name

def read_tmp_folder(parent_folder):
    all_names = []
    for lang in source_langs: 
        search_path = "{}/{}/tmp/**/*.title_main".format(parent_folder, lang)
        names_of_lang = glob.glob(search_path, recursive=True)
        names_of_lang = [name for name in names_of_lang if os.path.isfile(name)] 
        for name in names_of_lang:
            all_names.append((name, lang))
    return all_names

def read_tmp_list(tmp_list):
    all_names = []
    with open(tmp_list, "r") as f:
        lines = f.readlines()
    for line in lines:
        line = line.strip()
        all_names.append(line)
    return all_names

def get_accessed_files(accessed_file_list):
    all_names = []
    with open(accessed_file_list, "r") as f:
        lines = f.readlines()
    for line in lines:
        line = line.strip()
        if (len(line.split(' '))!=3):
            continue
        file_name, lang, status = line.split(' ')
        all_names.append((file_name, lang))
    return all_names

def read_ja_translated_folder(parent_folder):
    all_names = []
    for lang in source_langs: 
        search_path = "{}/{}/{}/**/*.html".format(parent_folder, lang, loc_phrase)
        names_of_lang = glob.glob(search_path, recursive=True)
        names_of_lang = [name for name in names_of_lang if os.path.isfile(name)] 
        for name in names_of_lang:
            all_names.append((name, lang))
    return all_names

def new_tmp(name):
    target_name = name.replace("/{}/".format(loc_phrase),"/{}_tmp/".format(loc_phrase),1)
    target_name = str(Path(target_name).with_suffix(".txt"))
    return target_name

def delete_old_ja_translated(parent_folder):
    global source_langs
    source_langs = ["cn", "es", "eu", "us", "int", "de", 'in', "kr", "jp", "fr"]
    delete_num =0
    #all_ja_translated = read_ja_translated_folder(parent_folder) # (path_to_tmp_content_file, lang)
    accessed_files = get_accessed_files(accessed_file_list)
    accessed_files_dic = {}

    for name, lang in accessed_files:
        target_name = source2target(name)
        html_name = str(Path(target_name).with_suffix(".html"))
        txt_name = str(Path(target_name).with_suffix(".txt"))
        new_html_name = new_tmp(html_name)
        new_html_name = str(Path(new_html_name).with_suffix(".html"))
        new_txt_name = str(Path(new_html_name).with_suffix(".txt"))

        new_dir = os.path.dirname(new_html_name)
        if (os.path.exists(new_dir) == False):
            os.makedirs(new_dir)

        command = "cp {} {}".format(html_name, new_html_name)
        os.system(command)
        command = "cp {} {}".format(txt_name, new_txt_name)
        os.system(command)
    return

    for name in accessed_files:
        accessed_files_dic[name[0]]=1
    for name, lang in all_ja_translated:
        source_name = ja_translate2source(name)
        if (accessed_files_dic.get(source_name, 0) == 0):
            delete_num += 1
            html_name = name
            txt_name = str(Path(html_name).with_suffix(".txt"))
            command = "rm -f {}".format(html_name)
            #os.system(command)
            command = "rm -f {}".format(txt_name)
            #os.system(command)
            os.remove(html_name)
            os.remove(txt_name)

def get_unaccessed_tmp_content(parent_folder):
    #all_tmp_content = read_tmp_folder(parent_folder) # (path_to_tmp_content_file, lang)
    all_tmp_content = read_tmp_list(tmp_list) # (path_to_tmp_content_file, lang)
    all_tmp_content = [name for name in all_tmp_content if (get_lang(name) in source_langs)]
    accessed_files = get_accessed_files(accessed_file_list)
    accessed_files_dic = {}
    for name in accessed_files:
        accessed_files_dic[name[0]]=1
    unaccessed_tmp = [name for name in all_tmp_content if accessed_files_dic.get(name, 0) == 0]
    unaccessed_tmp = [str(Path(source2tmp(name)).with_suffix(".title_main")) for name in unaccessed_tmp]
    return unaccessed_tmp

def get_unaccessed_tmp_with_lang_site_limit(parent_folder, lang, limit_num) -> List[str]:
    # all tmp: extracted
    # accessed: (en_)trans_list.txt
    # unaccessed: tmp - accessed -> filter
    lang_pattern = "{}/{}".format(parent_folder, lang)
    site_pattern = "{}/{}/.*?/(.*?)/.*".format(parent_folder, lang)
    site_pattern = re.compile(site_pattern)

    all_tmp_content = read_tmp_list(tmp_list) # (path_to_tmp_content_file, lang)
    all_tmp_content = [name for name in all_tmp_content if (get_lang(name) ==lang)]
    accessed_files = get_accessed_files(accessed_file_list)
    accessed_files_dic = {}
    for name in accessed_files:
        accessed_files_dic[name[0]]=1
    unaccessed_tmp = [name for name in all_tmp_content if accessed_files_dic.get(name, 0) == 0]
    unaccessed_tmp = [str(Path(source2tmp(name)).with_suffix(".title_main")) for name in unaccessed_tmp]
    unaccessed_tmp.reverse()

    filtered_tmp = []
    site_num = {}
    for name in unaccessed_tmp:
        site_res = re.match(site_pattern, name)
        if (site_res==None):
            continue
        site = site_res.group(1)
        site_num[site] = site_num.get(site, 0) + 1
        if (site_num[site]<=limit_num):
            filtered_tmp.append(name)
    return filtered_tmp 
    

def translator_part(input_part, lang):
    current_lang = map_to_lang.get(lang, lang)
    if (current_lang == tolang):
        return input_part

    global KEY, NAME, SECRET, account_index, account_len, consumer
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
    except:
        print ("Error from Minhon")
        print (lang)
        time.sleep(1500)
        return '' 

def translate_one(tmp_content, lang) -> Tuple[str, int]:
    # lang here means region actually...
    with open(tmp_content, "r") as f:
        input_text = f.read()
    input_parts, line_num = cut_parts(input_text)
    current_lang = map_to_lang.get(lang, lang)
    if (current_lang != tolang and line_num > 500):
        return '', 7
    output_parts = []
    for part in input_parts:
        output_parts.append(translator_part(part, lang))
    output_text = ''.join(output_parts)
    return output_text, 0

def save_result(output_text, target_txt):
    target_html = str(Path(target_txt).with_suffix(".html"))

    status = 0
    if (len(output_text)<=1):
        return 3

    save_path_directory = os.path.dirname(target_txt)
    if (os.path.exists(save_path_directory) == False):
        try:
            os.makedirs(save_path_directory)
        except:
            return 6
    with open(target_txt, "w") as f:
        f.write(output_text)

    output_line = ''
    try:
        #update_record(self, input_file, index, is_data_stream=False)
        res = es_importer.update_record(target_txt, index=es_index)
        res = es_importer.update_record(target_txt, index=es_doc_index, is_data_stream=True)
        output_line = "{} {}".format(target_txt.strip(), res)
    except:
        output_line = "{} {}".format(target_txt.strip(), 'error')
    with open(elastic_log, "a+") as f:
        f.write(output_line+'\n')

    output_html = text_to_html(output_text)
    with open(target_html, "w") as f:
        f.write(output_html)
    return 0

def write_to_accessed(unaccessed_tmp_content):
    global parent_folder
    for tmp_content in unaccessed_tmp_content:
        tmp_meta = str(Path(tmp_content).with_suffix(".meta"))
        source_html = tmp2source(tmp_content)
        lang = get_lang(source_html)
        target_txt = tmp2target(tmp_content)
        #if (os.path.exists(target_txt)):
        #    continue

        status=3
        if (has_all_file(tmp_content) == 0):
            status = 4
        with open(accessed_file_list, "a+") as f:
            line = "{} {} {}".format(source_html, lang, status)
            f.write(line.strip()+'\n')

def write_to_accessed_line(source_html, lang, status):
    with open(accessed_file_list, "a+") as f:
        line = "{} {} {}".format(source_html, lang, status)
        f.write(line.strip()+'\n')

def translate_all(unaccessed_tmp_content):
    global parent_folder
    for tmp_content in unaccessed_tmp_content:

        tmp_meta = str(Path(tmp_content).with_suffix(".meta"))
        source_html = tmp2source(tmp_content)
        lang = get_lang(source_html)
        target_txt = tmp2target(tmp_content)
        print ("Input: {}".format(tmp_content))

        if (os.path.exists(target_txt)):
            print ("exists")
            write_to_accessed_line(source_html, lang, 5)
            continue

        if (has_all_file(tmp_content) == 0):
            print ("no all file")
            write_to_accessed_line(source_html, lang, status=4)
            continue

        with open(tmp_meta, "r") as f:
            try:
                line = f.readlines()[0].strip()
            except:
                print ("no meta")
                write_to_accessed_line(source_html, lang, status=4)
                continue
            translated_flag, timestamp, lang, content_num, link_num, keyword_flag = line.split(' ')
            translated_flag = int(translated_flag)
            timestamp = float(timestamp)
            content_num = int(content_num)
            link_num = int(link_num)
            keyword_flag = (keyword_flag != "False")

        status = 0 # 0:ok 1: no keyword 2: too many links 3: others 4: not all files 5: exists 6: no permission
        if (keyword_flag == 0):
            status = 1
            print ("no keyword")
            #print (tmp_content)
            write_to_accessed_line(source_html, lang, status)
            continue
        #elif (float(content_num+1)/float(content_num+link_num+1)<0.33):
        elif (float(content_num+1)/float(content_num+link_num+1)<0.2):
            status = 2
            print ("many links")
            #print (tmp_content)
            write_to_accessed_line(source_html, lang, status)

        if (status == 0):
            output_text, status = translate_one(tmp_content, lang)
            if (status == 7):
                print ("file too long")
                write_to_accessed_line(source_html, lang, status)
                continue
            status = save_result(output_text, target_txt)
            if (status == 6):
                print ("no permission")
                write_to_accessed_line(source_html, lang, status)
                continue

            write_to_accessed_line(source_html, lang, status)
            target_html = str(Path(target_txt).with_suffix(".html"))
            year, month, day, hour, minute = my_get_time()
            minute = 0
            log_file_base = "{}-{:04d}-{:02d}-{:02d}-{:02d}-{:02d}.txt".format(log_file_base_lang, year, month, day, hour, minute)
            log_file  = "{}/{}".format(log_folder, log_file_base)
            with open(log_file, "a+") as f:
                f.write(target_html.strip()+'\n')
            print ("Output: {}".format(target_txt))

def revise_meta(parent_folder):
    keyword_false = 0
    keyword_true = 0
    new_accessed_file_list = []
    source_langs = ["cn", "es", "eu", "us", "int", "de", 'in', "kr", "jp", "fr"]
    with open(accessed_file_list, "r") as f:
        lines = f.readlines()
    for line in lines:
        source_html, lang, status = line.strip().split()
        status = int(status)
        tmp_html = source2tmp(source_html)
        tmp_meta = str(Path(tmp_html).with_suffix(".meta"))
        with open(tmp_meta, "r") as f:
            line = f.readlines()[0].strip()
            translated_flag, timestamp, lang, content_num, link_num, keyword_flag = line.split(' ')
            translated_flag = int(translated_flag)
            timestamp = float(timestamp)
            content_num = int(content_num)
            link_num = int(link_num)
            if (keyword_flag == "False"):
                keyword_flag = 0
                keyword_false+=1
                print (tmp_html)
            else:
                keyword_true+=1
                keyword_flag = 1
            if (keyword_flag == 0):
                status = 1
                target_txt = tmp2target(tmp_html)
                target_html = str(Path(target_txt).with_suffix(".html"))
                #print (target_txt)
                try:
                    os.remove(target_txt)
                    os.remove(target_html)
                except:
                    0
            else:
                status = status
        line = "{} {} {}".format(source_html, lang, status)
        new_accessed_file_list.append(line)
        new_accessed_file_list_line = '\n'.join(new_accessed_file_list)
    with open(accessed_file_list_tmp, "w") as f:
        f.write(new_accessed_file_list_line)
    print(keyword_false, keyword_true)

def has_all_file(tmp):
    tmp_meta = str(Path(tmp).with_suffix(".meta"))
    tmp_title_main = str(Path(tmp).with_suffix(".title_main"))
    tmp_block = str(Path(tmp).with_suffix(".block"))
    if (os.path.isfile(tmp_meta) and os.path.isfile(tmp_title_main) and os.path.isfile(tmp_block)):
        return 1
    else:
        return 0


if (tolang == 'ja'):
    accessed_file_list = ja_accessed_file_list
    URLs = Ja_URLs
    loc_phrase = "ja_translated"
    log_file_base_lang = "new-translated-files"
elif (tolang == 'en'):
    accessed_file_list = en_accessed_file_list
    URLs = En_URLs
    loc_phrase = "en_translated"
    log_file_base_lang = "new-translated-files-en"

print (tolang, accessed_file_list)
max_trans_page = 1000 
epoch_trans_page = 100 
limit_each_site = 5 # in each iteration translate only 10
while (1):
    for lang in source_langs:
        print ("Begin loop")
        unaccessed_tmp_content = get_unaccessed_tmp_with_lang_site_limit(parent_folder, lang, limit_each_site)
        print (lang, len(unaccessed_tmp_content))
        #if (len(unaccessed_tmp_content)>max_trans_page*1.5):
        #    unaccessed_tmp_content.sort(key = lambda x: priority(x))
        #    old_tmp = unaccessed_tmp_content[max_trans_page:]
        #    unaccessed_tmp_content = unaccessed_tmp_content[:max_trans_page]
        #    #print(old_tmp)
        #    #print (new_tmp)
        #    write_to_accessed(old_tmp)

        #print ("Got remaining")
        #if (len(unaccessed_tmp_content)>epoch_trans_page):
        #    unaccessed_tmp_content.sort(key = lambda x: priority(x))
        #    #random.shuffle(unaccessed_tmp_content)
        #    unaccessed_tmp_content = unaccessed_tmp_content[:epoch_trans_page]
        #r=len(unaccessed_tmp_content)
        
        translate_all(unaccessed_tmp_content)

    time.sleep(1)

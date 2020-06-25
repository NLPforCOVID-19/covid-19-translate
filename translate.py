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

# constant
source_langs = ["cn", "es", "eu", "us", "int", 'in', "kr", "jp", "de", "fr"]
source_langs_ori = source_langs
parent_folder = '/mnt/hinoki/share/covid19/html'
log_folder = '/mnt/hinoki/share/covid19/run/new-translated-files'
accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/trans_list.txt' 
accessed_file_list_tmp = '/mnt/hinoki/share/covid19/run/trans_log_song/trans_list_tmp.txt' 
tmp_list = '/mnt/hinoki/share/covid19/run/trans_log_song/extract_list.txt' 

# translation settings
NAME1 = 
KEY1=
SECRET1=

NAME2 =
KEY2 =
SECRET2 =

NAME3 =
KEY3 =
SECRET3 =

account_list = [ (NAME1, KEY1, SECRET1), (NAME2, KEY2, SECRET2), (NAME3, KEY3, SECRET3)]
account_len = len(account_list)
account_index = int(sys.argv[1])

NAME, KEY, SECRET = account_list[account_index] 
#source_langs = [source_langs[account_index]]
if (account_index == 0):
    source_langs = source_langs[:-2]
elif (account_index == 1):
    source_langs = source_langs[:-2]
elif (account_index == 2):
    source_langs = source_langs[-2:]
print (source_langs)

consumer = OAuth1(KEY, SECRET)
En_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_en_ja/'
#Zh_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_zh-CN_ja/'
Zh_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalN_zh-CN_ja/'
Ko_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/patentN_ko_ja/'
Fr_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalN_fr_ja/'
Es_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalN_es_ja/'
De_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalN_de_ja/'
URLs = {"en":En_Ja_URL, "eu":En_Ja_URL, "us":En_Ja_URL, "int":En_Ja_URL, "zh":Zh_Ja_URL, "cn":Zh_Ja_URL, "kr":Ko_Ja_URL, "fr":Fr_Ja_URL, "es": Es_Ja_URL, "de": De_Ja_URL, 'in': En_Ja_URL}


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

def cut_parts(input_text):
    """
        cut input_text into shorter parts
    """
    lines = input_text.split('\n')
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
    return parts

def tmp2source(tmp_name):
    source_name = tmp_name.replace("/tmp/","/orig/",1)
    source_name = str(Path(source_name).with_suffix(".html"))
    return source_name
    
def source2tmp(tmp_name):
    source_name = tmp_name.replace("/orig/","/tmp/",1)
    source_name = str(Path(source_name).with_suffix(".html"))
    return source_name

def source2target(tmp_name):
    source_name = tmp_name.replace("/orig/","/ja_translated/",1)
    source_name = str(Path(source_name).with_suffix(".html"))
    return source_name

def tmp2target(tmp_name):
    target_name = tmp_name.replace("/tmp/","/ja_translated/",1)
    target_name = str(Path(target_name).with_suffix(".txt"))
    return target_name

def ja_translate2source(name):
    target_name = name.replace("/ja_translated/","/orig/",1)
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
        search_path = "{}/{}/ja_translated/**/*.html".format(parent_folder, lang)
        names_of_lang = glob.glob(search_path, recursive=True)
        names_of_lang = [name for name in names_of_lang if os.path.isfile(name)] 
        for name in names_of_lang:
            all_names.append((name, lang))
    return all_names

def new_tmp(name):
    target_name = name.replace("/ja_translated/","/ja_translated_tmp/",1)
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
    if (lang == 'jp'):
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
    except Exception as e:
        print ("Error from Minhon:")
        print('type:' + str(type(e)))
        print('args:' + str(e.args))
        print('e:' + str(e))
        print (lang)
        sleep(1500)
        return '' 

def translate_one(tmp_content, lang):
    with open(tmp_content, "r") as f:
        input_text = f.read()
    input_parts = cut_parts(input_text)
    output_parts = []
    for part in input_parts:
        output_parts.append(translator_part(part, lang))
    output_text = ''.join(output_parts)
    return output_text

def save_result(output_text, target_txt):
    target_html = str(Path(target_txt).with_suffix(".html"))

    status = 0
    if (len(output_text)<=1):
        return 3

    save_path_directory = os.path.dirname(target_txt)
    if (os.path.exists(save_path_directory) == False):
        os.makedirs(save_path_directory)
    with open(target_txt, "w") as f:
        f.write(output_text)

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

def translate_all(unaccessed_tmp_content):
    global parent_folder
    for tmp_content in unaccessed_tmp_content:

        tmp_meta = str(Path(tmp_content).with_suffix(".meta"))
        source_html = tmp2source(tmp_content)
        lang = get_lang(source_html)
        target_txt = tmp2target(tmp_content)
        if (os.path.exists(target_txt)):
            continue

        if (has_all_file(tmp_content) == 0):
            status = 4
            with open(accessed_file_list, "a+") as f:
                line = "{} {} {}".format(source_html, lang, status)
                f.write(line.strip()+'\n')
            continue

        with open(tmp_meta, "r") as f:
            try:
                line = f.readlines()[0].strip()
            except:
                status = 4
                with open(accessed_file_list, "a+") as f:
                    line = "{} {} {}".format(source_html, lang, status)
                    f.write(line.strip()+'\n')
                continue
            translated_flag, timestamp, lang, content_num, link_num, keyword_flag = line.split(' ')
            translated_flag = int(translated_flag)
            timestamp = float(timestamp)
            content_num = int(content_num)
            link_num = int(link_num)
            if (keyword_flag == "False"):
                keyword_flag = 0
            else:
                keyword_flag = 1

        status = 0 # 0:ok 1: no keyword 2: too many links 3: others
        if (keyword_flag == 0):
            status = 1
        elif (float(content_num+1)/float(content_num+link_num+1)<0.2):
            status = 2
        else:
            status = 0

        if (status == 0):
            output_text = translate_one(tmp_content, lang)
            status = save_result(output_text, target_txt)

            year, month, day, hour, minute = my_get_time()
            minute = 0
            log_file_base = "new-html-files-{:04d}-{:02d}-{:02d}-{:02d}-{:02d}.txt".format(year, month, day, hour, minute)
            log_file  = "{}/{}".format(log_folder, log_file_base)
            with open(log_file, "a+") as f:
                f.write(target_txt.strip()+'\n')



        with open(accessed_file_list, "a+") as f:
            line = "{} {} {}".format(source_html, lang, status)
            f.write(line.strip()+'\n')
        
        if (status == 0):
            print ("Input: {}\nOutput: {}".format(tmp_content, target_txt))

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

    time.sleep(10)

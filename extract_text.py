# usage here: https://github.com/Alir3z4/html2text/blob/master/docs/usage.md
from typing import List, Tuple
import os
import sys
import re
import time
import json
import glob
import random
from pathlib import Path
from datetime import datetime, timedelta
from utils import get_base_tag, check_list_page, get_html_file

from bs4 import BeautifulSoup
import html2text

import requests as req
from requests_oauthlib import OAuth1
from xml.etree.ElementTree import *

source_langs = ["jp", "cn", "br", "kr", "es", "eu", "us", "int", 'in', "de", "fr", "au", 'za', 'np', 'my']
source_langs_ori = source_langs
keywords = ["COVID", "covid", "肺炎", "コロナ", "corona", "Corona", "코로나"] # En, zh, ja, fr, ko

parent_folder = '/mnt/hinoki/share/covid19/html'
html_folder = "/mnt/hinoki/share/covid19/run/new-html-files"
extract_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/extract_list.txt'

gene_block_command = "/home/song/git/WWW2sf/tool/html2sf.sh -T -D /home/song/git/detectblocks"

year_month_day_hour_pattern = re.compile(".*?/(\d\d\d\d)/(\d\d)/(\d\d)-(\d\d).*")

lang_index = int(sys.argv[1])
source_langs = [source_langs[lang_index]]

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

def get_lang(input_path):
    global parent_folder
    for lang in source_langs_ori:
        detect_part = "{}/{}/".format(parent_folder, lang)
        if (detect_part in input_path):
            return lang
    return None

def get_accessed_files(extract_accessed_file_list):
    all_names = {} 
    with open(extract_accessed_file_list, "r") as f:
        lines = f.readlines()
    for line in lines:
        line = line.strip()
        all_names[line]=1
    return all_names

def contain_keyword(input_text):
    for keyword in keywords:
        if (keyword in input_text):
            return True
    return False

def first_h1_text(input_html):
    with open(input_html) as f:
        soup = BeautifulSoup(f, 'html.parser')
    first_h1 = soup.find('h1')
    if (first_h1 != None):
        return (first_h1.text)
    else:
        return ''

def extract_source_text_xml(xml_file_name):
    content_num = 0
    link_num = 0

    result_list = []
    with open(xml_file_name, "r") as f:
        soup = BeautifulSoup(f, "xml")

    try:
        title = soup.find("Title").find("RawString").string
        result_list.append(title)
    except:
        title = ''

    for item in soup.find_all("S"):
        res = item["BlockType"]
        if (res == "maintext" or res == "unknown_block"):
            content_num += 1
            content = item.find("RawString").string
            result_list.append(content)

        else:
            link_num += 1

    result = "\n".join(result_list)
    return result, content_num, link_num

def read_all_input_htmls(parent_folder):
    """
        return list of (url, lang) of all htmls in the parent folder
    """
    all_names = []
    for lang in source_langs: 
        search_path = "{}/{}/orig/**/*.html".format(parent_folder, lang)
        names_of_lang = glob.glob(search_path, recursive=True)
        names_of_lang = [name for name in names_of_lang if os.path.isfile(name)] 
        for name in names_of_lang:
            all_names.append((name, lang))
    return all_names

def read_all_input_htmls_with_lang(parent_folder, lang):
    """
        return list of (url, lang) of all htmls in the parent folder
    """
    all_names = []
    search_path = "{}/{}/orig/**/*.html".format(parent_folder, lang)
    #search_path = "{}/{}/orig/www.gouvernement.fr/**/*.html".format(parent_folder, lang)
    names_of_lang = glob.glob(search_path, recursive=True)
    names_of_lang = [name for name in names_of_lang if os.path.isfile(name)] 
    for name in names_of_lang:
        all_names.append((name, lang))
    return all_names

def read_all_input_htmls_with_folder(folder):
    """
        return list of (url, lang) of all htmls in the parent folder
    """
    all_names = []
    search_path = "{}/**/*.html".format(folder)
    names_of_lang = glob.glob(search_path, recursive=True)
    names_of_lang = [name for name in names_of_lang if os.path.isfile(name)] 
    for name in names_of_lang:
        all_names.append((name, lang))
    return all_names

def read_all_input_htmls_with_lang_and_site_limit(parent_folder, lang, limit_num, day_limit=7) -> List[Tuple[str, str]]:
    """
        return htmls of lang within day_limit, maximum limit_num for each site
    """
    lang_pattern = "{}/{}".format(parent_folder, lang)
    site_pattern = "{}/{}/orig/(.*?)/.*".format(parent_folder, lang)
    site_pattern = re.compile(site_pattern)

    all_names = []
    old_names = []
    site_num = {}

    for i in range(0, day_limit):
        dt = datetime.now() - timedelta(i)
        year, month, day = dt.year, dt.month, dt.day
        log_files_pattern = "{}/*{:4d}-{:02d}-{:02d}*.txt".format(html_folder, year, month, day)
        file_names = glob.glob(log_files_pattern, recursive=True)

        for file_name in file_names:
            with open(file_name, "r") as f:
                lines = f.readlines()
                for line in lines:
                    line = line.strip()
                    if (lang_pattern not in line):
                        continue
                    site_res = re.match(site_pattern, line)
                    if (site_res == None):
                        continue
                    site = site_res.group(1)
                    site_num[site] = site_num.get(site, 0) + 1
                    if (site_num[site]<=limit_num):
                        all_names.append((line, lang))
                    else:
                        old_names.append((line, lang))
    return all_names

def line_link_page(html_file):
    soup = get_html_file(html_file)
    base_tag, base_text = get_base_tag(soup)
    res = check_list_page(base_tag)
    return res


def gene_tmp(name, lang):
    tmp_name = name.replace("/{}/orig/".format(lang), "/{}/tmp/".format(lang))
    tmp_name_dir = os.path.dirname(tmp_name)
    if (os.path.exists(tmp_name_dir) == 0):
        os.makedirs(tmp_name_dir)

    block_name = str(Path(tmp_name).with_suffix(".block"))
    title_main_name = str(Path(tmp_name).with_suffix(".title_main"))
    meta_name = str(Path(tmp_name).with_suffix(".meta"))
    #abs_name = turn_to_abs(name, /mnt/hinoki/share/covid19)

    ##fix
    #result, content_num, link_num = extract_source_text_xml(block_name)
    #percent = float(link_num+1)/float(link_num+content_num+1)
    #if (percent>0.66 and percent<0.8):
    #    print ("test percent")
    #    print (name)
    #    print (percent)
    #return

    if (os.path.exists(meta_name) == 1):
        with open(extract_accessed_file_list, "a+") as f:
            f.write(name.strip()+'\n')
        return

    if (lang=='jp'):
        command = "{} '{}' > '{}'".format(gene_block_command, name, block_name)
    elif (lang == 'cn'):
        command = "{} -f '{}' > '{}'".format(gene_block_command, name, block_name)
    else:
        command = "{} -E '{}' > '{}'".format(gene_block_command, name, block_name)

    try:
        #print (command)
        os.system(command)
        result, content_num, link_num = extract_source_text_xml(block_name)
        if ('zusammengegencorona' in name):
            h1_text = first_h1_text(name) 
            with open(title_main_name, "w") as f:
                if (len(h1_text)>0):
                    f.write(h1_text.strip()+'\n')
                f.write(result)
        else:
            with open(title_main_name, "w") as f:
                f.write(result)

        translated_flag = 0
        timestamp = os.path.getmtime(name)
        lang = lang
        keyword_flag = contain_keyword(result)

        #link_page_flag = line_link_page(name)
        #print (name)
        #print (link_page_flag)
        #print (content_num, link_num)
        #if (link_page_flag==True):
        #    content_num=1
        #    link_num=100
        meta_info = "{} {} {} {} {} {}".format(translated_flag, timestamp, lang, content_num, link_num, keyword_flag)

        with open(meta_name, "w") as f:
            f.write(meta_info)
        tot_num+=1
        with open(extract_accessed_file_list, "a+") as f:
            f.write(name.strip()+'\n')

        #print ("New trans {} of lang {}".format(tot_num, lang))
        #print ("block generated: {}".format(block_name))
        #print ("title_main generated: {}".format(title_main_name))
        #print ("meta generated: {}".format(meta_name))
    except:
        with open(extract_accessed_file_list, "a+") as f:
            f.write(name.strip()+'\n')
        return

def write_to_access(names):
    for name,lang in names:
        with open(extract_accessed_file_list, "a+") as f:
            f.write(name.strip()+'\n')


limit_num = 1000
while (1):
    start_time = time.time()
    tot_num = 0
    extract_accessed_files = get_accessed_files(extract_accessed_file_list)
    #print ("# in each lang: {}".format(now_num))
    for lang in source_langs:
        all_names = read_all_input_htmls_with_lang_and_site_limit(parent_folder, lang, limit_num)
        #fix
        before_filter = len(all_names)
        all_names = [name for name in all_names if (extract_accessed_files.get(name[0], 0)==0)]
        after_filter = len(all_names)

        if (after_filter == 0):
            time.sleep(10)
            continue
        print ("Got files")
        print (after_filter)


        #if (after_filter > limitation):
        #    #all_names.sort(key = lambda x: priority(x))
        #    random.shuffle(all_names)
        #    old_names = all_names[limitation:]
        #    all_names = all_names[:limitation]
        #    write_to_access(old_names)

        #print ("Language: {} with {} files".format(lang, len(all_names)))
        #all_names = all_names[:now_num]
        #all_names.sort(key = lambda x: -os.path.getmtime(x[0]))

        for i, (name,lang) in enumerate(all_names):
            print (i, name)
            gene_tmp(name, lang)

    end_time = time.time()
    elapsed_time = int(end_time-start_time)
    #sleep_time = 3600*3 - elapsed_time
    #sleep_time = 10 
    #if (sleep_time < 0):
    #    sleep_time = 10
    #time.sleep(sleep_time)
    time.sleep(10)

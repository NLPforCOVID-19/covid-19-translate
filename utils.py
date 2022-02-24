import os
import sys
from typing import List, Tuple
import codecs
import time
import json
import glob
import random
import re
from datetime import datetime, timedelta
from pathlib import Path

from pyknp import Juman

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
import html2text

import requests as req
from requests_oauthlib import OAuth1
from xml.etree.ElementTree import *

from elastic_search_utils import ElasticSearchImporter, ElasticSearchTwitterImporter

error_log_file = '/home/song/covid19-minhon/log/error.log'

# constants
# keywords translation found here: http://www.clair.or.jp/tabunka/portal/info/contents/114517.php
keywords = ["COVID", "covid", "新冠","新型冠状病毒", "コロナ", "corona", "Corona", "CORONA", "코로나", "코로나바이러스", "โควิด", "SARS-CoV-2", "коронавирусной"] # En, zh, ja, fr, ko

domains = ["jp", "cn", "br", "kr", "mx", "eu", "us", "my", "int", 'in', "de", "au", 'za', 'np','sg', 'gb', 'id', "fr", "ru", "af", "it", "th", "es", "pt", "ca"]
domain_to_lang = {
"jp":"ja",
"cn":"zh",
"zh-cn":"zh",
"pt":"pt",
"br":"pt",
"kr":"ko",
"es":"es",
"mx":"es",
"my":"id",
"de":"de",
"id":"id",
"fr":"fr",
"ru":"ru",
"it":"it",
"th":"th",
"en":"en",
"ca":"en",
"us":"en",
"eu":"en",
"int":"en",
"in":"en",
"au":"en",
"za":"en",
"np":"en",
"sg":"en",
"af":"en",
"gb":"en",
}

# anylang -> Ja
En_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_en_ja/'
Zh_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_zh-CN_ja/'
Ko_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ko_ja/'
Fr_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_fr_ja/'
Es_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_es_ja/'
De_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_de_ja/'
Pt_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_pt_ja/'
Id_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_id_ja/'
Ru_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ru_ja/'
It_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_it_ja/'
Th_Ja_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_th_ja/'

# anylang -> En
Ja_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ja_en/'
Zh_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_zh-CN_en/'
Ko_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ko_en/'
Fr_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_fr_en/'
Es_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_es_en/'
De_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_de_en/'
Pt_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_pt_en/'
Id_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_id_en/'
Ru_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_ru_en/'
It_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_it_en/'
Th_En_URL = 'https://mt-auto-minhon-mlt.ucri.jgn-x.jp/api/mt/generalNT_th_en/'

# lang -> Ja
Ja_URLs = {
    "ja":"",
    "en":En_Ja_URL,
    "zh":Zh_Ja_URL,
    "ko":Ko_Ja_URL,
    "fr":Fr_Ja_URL,
    "es":Es_Ja_URL,
    "de":De_Ja_URL,
    "pt":Pt_Ja_URL,
    "id":Id_Ja_URL,
    "ru":Ru_Ja_URL,
    "it":It_Ja_URL,
    "th":Th_Ja_URL
}
# lang -> En
En_URLs = {
    "en":"",
    "ja":Ja_En_URL,
    "zh":Zh_En_URL,
    "ko":Ko_En_URL,
    "fr":Fr_En_URL,
    "es":Es_En_URL,
    "de":De_En_URL,
    "pt":Pt_En_URL,
    "id":Id_En_URL,
    "ru":Ru_En_URL,
    "it":It_En_URL,
    "th":Th_En_URL
}
   
# translate core functions
def translate_part(input_part, from_lang, to_lang, URL, KEY, NAME, consumer) -> str:
    """
    translate text from from_lang to to_lang, where the length of input_part is limited to 300 characters
    return translated text
    """
    if (from_lang == to_lang):
        return input_part
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
        translated_sentence = result['result']['text']
        return translated_sentence
    except:
        print ("Error from Minhon")
        time.sleep(1500)
        return '' 

def translate_text(input_text, from_lang, to_lang, URL, KEY, NAME, consumer) -> Tuple[str, int]:
    """
    translate text from from_lang to to_lang, 
    return translated text and the status, 0 normal others error
    """
    if (from_lang == to_lang):
        return input_text, 0

    input_parts, line_num = cut_parts(input_text)
    if (line_num > 500): return '', -1 

    output_parts = [translate_part(part, from_lang, to_lang, URL, KEY, NAME, consumer) for part in input_parts]
    output_text = ''.join(output_parts)
    return output_text, 0

# elastic core function
def save_elastic(es_importer, es_index, es_doc_index, target_txt, elastic_log):
    output_line = ''
    try:
        res = es_importer.update_record(target_txt, index=es_index)
        res = es_importer.update_record(target_txt, index=es_doc_index, is_data_stream=True)
        output_line = "{} {}".format(target_txt.strip(), res)
    except:
        print ("In translation, elastic error")
        output_line = "{} {}".format(target_txt.strip(), 'error')
    with open(elastic_log, "a+") as f:
        f.write(output_line+'\n')

# file path 
def tmp2target(tmp_name, loc_phrase):
    target_name = tmp_name.replace("/tmp/","/{}/".format(loc_phrase),1)
    target_name = str(Path(target_name).with_suffix(".txt"))
    return target_name

def tmp2source(tmp_name):
    source_name = tmp_name.replace("/tmp/","/orig/",1)
    source_name = str(Path(source_name).with_suffix(".html"))
    return source_name
    
def source2tmp(tmp_name):
    source_name = tmp_name.replace("/orig/","/tmp/",1)
    source_name = str(Path(source_name).with_suffix(".html"))
    return source_name

def source2target(tmp_name, loc_phrase):
    source_name = tmp_name.replace("/orig/","/{}/".format(loc_phrase),1)
    source_name = str(Path(source_name).with_suffix(".html"))
    return source_name

def ja_translate2source(name, loc_phrase):
    target_name = name.replace("/{}/".format(loc_phrase),"/orig/",1)
    target_name = str(Path(target_name).with_suffix(".html"))
    return target_name

# time
def my_get_time():
    dt = datetime.now()
    year, month, day, hour, minute = dt.year, dt.month, dt.day, dt.hour, dt.minute
    return year, month, day, hour, minute

# file funcions
def read_line_from_file(file):
    with open(file, "r") as f:
        lines = f.readlines()
    return lines[0].strip()

def read_line_list_from_file(input_file) -> List[str]:
    """
    create a list containing all lines in the file
    """
    with open(input_file, "r") as f:
        lines = f.readlines()
    return [line.strip() for line in lines]

def write_to_accessed_line(accessed_file_list, orig_file, lang, status):
    with open(accessed_file_list, "a+") as f:
        line = "{} {} {}".format(orig_file, lang, status)
        f.write(line.strip()+'\n')

def write_to_error_log_file(text, error_log_file=error_log_file):
    with open(error_log_file, "a+") as f:
        f.write(text.strip()+'\n')

def get_text_from_html(input_html):
    """
    get text from html
    """
    with open(input_html) as f:
        soup = BeautifulSoup(f, 'html.parser')
    first_p = soup.find('p')
    if (first_p != None):
        return (first_p.text)
    else:
        return ""

# text functions
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

def text_to_html(text):
    """
    convert text to html
    """
    lines = text.split('\n')
    title = '<head><meta charset="utf-8"/></head>'
    content = lines
    output_html = "<html>\n{}\n<body>\n".format(title)
    for line in content:
        line = "<p>{}</p>\n".format(line)
        output_html += line
    output_html += "</body>\n</html>\n"
    return output_html

def save_hashtag_url(text):
    """
    add special tokens ｟ ｠to the urls and hashtag in the text
    so that the translator don't translate them
    """
    text = re.sub("(https://(.*/)+.*? )", " ｟\g<1> ｠ ", text) # Remove urls from text.
    text = re.sub("([#＃]\w+\s*)", " ｟\g<1> ｠ ", text) # Remove hashtags from text.
    return text

def get_start_end_newlines(text):
    """
        if the start and end position contains \n, we save them
    """
    pattern = re.compile('^(\n*).*?(\n*)$')
    m = re.search(pattern, text)
    if (m!=None):
        return m.group(1), m.group(2)
    else:
        return "", ""

# check functions
def get_domain(parent_folder, input_path, possible_domains):
    for domain in possible_domains:
        detect_part = "{}/{}/".format(parent_folder, domain)
        if (detect_part in input_path):
            return domain 
    return None

def has_all_file(tmp):
    """
        check if all necessary files for translation exists
    """
    tmp_meta = str(Path(tmp).with_suffix(".meta"))
    tmp_title_main = str(Path(tmp).with_suffix(".title_main"))
    tmp_block = str(Path(tmp).with_suffix(".block"))
    if (os.path.isfile(tmp_meta) and os.path.isfile(tmp_title_main) and os.path.isfile(tmp_block)):
        return 1
    else:
        return 0

# read folder function
def read_tmp_folder(parent_folder):
    all_names = []
    for lang in source_langs: 
        search_path = "{}/{}/tmp/**/*.title_main".format(parent_folder, lang)
        names_of_lang = glob.glob(search_path, recursive=True)
        names_of_lang = [name for name in names_of_lang if os.path.isfile(name)] 
        for name in names_of_lang:
            all_names.append((name, lang))
    return all_names

# list function
def convert_list_to_dict(list_of_item):
    """
    return a dict with key as item and value as 1
    """
    d = {}
    for item in list_of_item:
        d[item] = 1
    return d

def list_item_not_in_dict(l, d):
    """
    return a list of item in l that not in keys of d
    """
    res = []
    for item in l:
        if (d.get(item, 0)==0):
            res.append(item)
    return res

# html functions
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
        soup = BeautifulSoup(f, "lxml-xml")
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

def clean_ja(text):
    r = len(text) - len(re.sub("[a-zA-Z]", "", text))
    if r > len(text)//2:
        return clean_en(text)
    ntext = re.sub("(?![a-zA-Z]) ", "", text.strip())
    ntext = ntext.replace("　", "").replace("\t", "").strip()
    ntext = re.sub("-丹波市ホームページ", "", ntext)
    ntext = re.sub("｜尼崎市公式ホームページ", "", ntext)
    ntext = re.sub("/むかわ町-北海道むかわ町公式ウェブサイト", "", ntext)
    return ntext

def clean_en(text):
    ntext = text.replace("\t", " ").strip()
    return ntext

def check_english(text):
    ntext = text.replace("　", "").replace(" ", "").replace("\t", "").strip()
    if len(ntext) < 200:
        return False
    ntext = ntext[:200]

    nal_num = len(re.sub("[a-zA-Z]", "", ntext))
    all_num = len(ntext)
    if float(all_num - nal_num) / all_num > 0.6:
        return True
    return False

def get_target_files(data_dir, ignores=[], total=None):
    p = Path(data_dir)
    # htmlファイルのうち、ターゲットのもの（faqとindex以外）だけを収集
    #files = [file for file in p.glob("**/*") if not (("index" in file.name) or ("faq" in file.name))]
    #for root, dirs, files in os.walk(root_dir):
    files = []
    dks = {".html", ".htm", ""}
    c = 0
    for file in p.glob("**/*"):
        flag = False
        for pattern in ignores:
            if re.search(pattern, str(file)):
                flag = True
        if flag:
            continue
        if re.search("\?", str(file)):
            continue
        if file.suffix not in dks:
            continue
        if not file.is_file():
            continue
        #print(file)
        files.append(file)
        if c == total:
            return files
        c += 1
    return(files)

def get_html_file(file):
    html_file = codecs.open(file, "r", 'utf-8', 'ignore')
    html = html_file.read()
    html_file.close()
    soup = BeautifulSoup(html, 'html.parser')
    return soup

def check_date_pattern(string):
    if re.search("[0-9]月", string):
        return True
    if re.search("20\d\d", string):
        return True
    if re.search("199\d", string):
        return True
    if re.search("(平成|昭和)", string):
        return True
    if re.search("H[2-3][0-9]", string):
        return True
    return False

def check_link_pattern(string):
    if re.search("リンク", string):
        return True
    return False

def check_list(node):
    # 箇条書きのうちリンクが多いか
    source = node["source"]
    soup = BeautifulSoup(source, 'html.parser')

    li_list = soup.find_all("li")
    if not li_list:
        return False

    p_list = soup.find_all("p")
    p_length = 0
    if p_list:
        for ptag in p_list:
            if ptag.find("a"):
                continue
            p_length += len(clean_ja(ptag.text))
    if p_length > 30:
        return False

    counter = 0
    a_counter = 0
    for li_tag in li_list:
        counter += 1
        if li_tag.find("a"):
            a_counter += 1
    if float(a_counter) / counter > 0.8:
        return True
    return False

def check_list_page(current_tag):
    p_length = a_length = 0
    for element in current_tag.next_elements:
        if isinstance(element, NavigableString):
            continue
        if element.name == "a":
            a_length += len(element.text)
        if element.name in {"strong", "p"}:
            p_length += len(element.text)
    if p_length == 0:
        return False
    if a_length > 2*p_length:
        return True
    return False

def line_link_page(html_file):
    """
    check_list_pageでリンクが多いかどうかを判定する
    """
    soup = get_html_file(html_file)
    base_tag, base_text = get_base_tag(soup)
    res = check_list_page(base_tag)
    return res

def get_base_tag(main_div):
    base_tag = htext = None
    _tn = 1
    while _tn < 10:
        h1s = list(main_div.find_all("h{}".format(_tn)))
        if not h1s:
            _tn += 1
            continue
        for htag in h1s:
            if htag.find("a"):
                continue
            if htag.text.strip():
                base_tag = htag
                htext = base_tag.text
                base_tag = base_tag.next_element
                break
        if base_tag:
            break
        _tn+=1
    return base_tag, htext


def levenshtein(s1, s2):
    n, m = len(s1), len(s2)

    dp = [[0] * (m + 1) for _ in range(n + 1)]

    for i in range(n + 1):
        dp[i][0] = i

    for j in range(m + 1):
        dp[0][j] = j

    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            dp[i][j] = min(dp[i - 1][j] + 1,         # insertion
                           dp[i][j - 1] + 1,         # deletion
                           dp[i - 1][j - 1] + cost)  # replacement

    return dp[n][m]

def word_distance_en(s1, s2):
    sss = [
            set(ss.split()) for ss in [s1, s2]
            ]
    if min(len(sss[0]), len(sss[1])) == 0:
        return 0
    return float(len(sss[0] & sss[1]))/min(len(sss[0]), len(sss[1]))

def word_distance(s1, s2):
    juman = Juman()
    r = len(s1+s2) - len(re.sub("[a-zA-Z0-9]", "", s1+s2))
    if r > len((s1+s2).replace(" ", ""))//2:
        return word_distance_en(s1, s2)
    sss = [
            set(
                [item.midasi for item in juman.analysis(ss).mrph_list() \
                        if item.hinsi in {'名詞', '動詞', '形容詞', '指示詞'}\
                        or '内容語' in item.imis
                        ]
            ) for ss in [s1, s2]
            ]
    if min(len(sss[0]), len(sss[1])) == 0:
        return 0
    return float(len(sss[0] & sss[1]))/min(len(sss[0]), len(sss[1]))

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


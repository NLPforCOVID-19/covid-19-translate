# usag here: https://github.com/Alir3z4/html2text/blob/master/docs/usage.md
from utils import *
from confidental_info import account_list 

# input arguments
account_index = int(sys.argv[1])
to_lang = sys.argv[2]

# path and constants
parent_folder = '/mnt/hinoki/share/covid19/html'
log_folder = '/mnt/hinoki/share/covid19/run/new-translated-files'
en_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/en_trans_list.txt' 
ja_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/trans_list.txt' 
accessed_file_list_tmp = '/mnt/hinoki/share/covid19/run/trans_log_song/trans_list_tmp.txt' 
tmp_list = '/mnt/hinoki/share/covid19/run/trans_log_song/extract_list.txt' 
log_file_base_lang = ''
elastic_log = '/mnt/hinoki/share/covid19/run/trans_log_song/elastic_log.txt'

# get unaccessed files
def get_accessed_files(accessed_file_list) -> List[str]:
    lines = open(accessed_file_list, "r").readlines()
    all_names = []
    for line in lines:
        line = line.strip()
        if (len(line.split(' '))!=3): continue
        file_name, domain, status = line.split(' ')
        all_names.append(file_name)
    return all_names

def get_unaccessed_tmp_content(parent_folder, domain):
    """
    for a specific domain, get all unaccessed files
    1. get all newly crawled files
    2. get all accessed files
    3. get all unaccessed files by crawled files - accessed files
    """
    all_tmp_content = read_line_list_from_file(tmp_list) 
    all_tmp_content = [name for name in all_tmp_content if (get_domain(parent_folder, name, domains)==domain)]

    accessed_files_list = get_accessed_files(accessed_file_list)
    accessed_files_dic = convert_list_to_dict(accessed_files_list)

    unaccessed_tmp = list_item_not_in_dict(all_tmp_content, accessed_files_dic)
    unaccessed_tmp = [str(Path(source2tmp(name)).with_suffix(".title_main")) for name in unaccessed_tmp]
    return unaccessed_tmp

def filter_list_with_site_limit(unaccessed_tmp, limit_num, site_pattern) -> List:
    """
    input: a list contains file names from different sites
    output: a list contains file names from different sites with limit_num from each site, detecting using site_pattern
    """
    filtered_tmp = []
    site_num = {}
    for name in unaccessed_tmp:
        site_res = re.match(site_pattern, name)
        if (site_res==None): continue
        site = site_res.group(1)
        site_num[site] = site_num.get(site, 0) + 1
        if (site_num[site]<=limit_num):
            filtered_tmp.append(name)
    return filtered_tmp 

def get_unaccessed_tmp_with_domain_site_limit(parent_folder, domain, limit_num) -> List[str]:
    unaccessed_tmp = get_unaccessed_tmp_content(parent_folder, domain)
    unaccessed_tmp.reverse()

    site_pattern = re.compile("{}/{}/.*?/(.*?)/.*".format(parent_folder, domain))
    filtered_tmp = filter_list_with_site_limit(unaccessed_tmp, limit_num, site_pattern)
    return filtered_tmp

def save_log(log_folder, target_html):
    year, month, day, hour, minute = my_get_time()
    minute = 0
    log_file_base = "{}-{:04d}-{:02d}-{:02d}-{:02d}-{:02d}.txt".format(log_file_base_lang, year, month, day, hour, minute)
    log_file  = "{}/{}".format(log_folder, log_file_base)
    with open(log_file, "a+") as f:
        f.write(target_html.strip()+'\n')

def save_result(output_text, target_txt):
    # handle errors first
    if (len(output_text)<=1):
        return 3
    save_path_directory = os.path.dirname(target_txt)
    if (os.path.exists(save_path_directory) == False):
        try:
            os.makedirs(save_path_directory)
        except:
            return 6
    try:
        es_importer.update_record(target_txt, index=es_index)
        es_importer.update_record(target_txt, index=es_doc_index, is_data_stream=True)
    except:
        return 8

    # save the output_text and output_html
    with open(target_txt, "w") as f:
        f.write(output_text)

    output_html = text_to_html(output_text)
    target_html_file = str(Path(target_txt).with_suffix(".html"))
    with open(target_html_file, "w") as f:
        f.write(output_html)
    return 0

def process_all(unaccessed_tmp_content):
    for tmp_content in unaccessed_tmp_content:
        status = 0 # 0:ok 1: no keyword 2: too many links 3: others 4: not all files 5: exists 6: no permission 7:file too long 8: elastic search 9: domain not defined

        source_html = tmp2source(tmp_content)
        tmp_meta = str(Path(tmp_content).with_suffix(".meta"))
        target_txt = tmp2target(tmp_content, loc_phrase)
        target_html = str(Path(target_txt).with_suffix(".html"))

        domain = get_domain(parent_folder, source_html, domains)
        from_lang = domain_to_lang.get(domain)

        guessed_lang_file = str(Path(source_html).with_suffix(".lang"))
        if (os.path.exists(guessed_lang_file)):
            from_lang = read_line_from_file(guessed_lang_file)

        try:
            if ("cn" in from_lang):
                from_lang = "cn"
            URL=URLs[from_lang]
        except:
            print ("no this domain")
            write_to_accessed_line(accessed_file_list, source_html, domain, status=9)
            continue

        print ("Article translation input: {}".format(tmp_content))

        if (os.path.exists(target_txt)):
            print ("exists")
            write_to_accessed_line(accessed_file_list, source_html, domain, status=5)
            continue

        if (has_all_file(tmp_content) == 0):
            print ("no all file")
            write_to_accessed_line(accessed_file_list, source_html, domain, status=4)
            continue

        try:
            with open(tmp_meta, "r") as f:
                line = f.readlines()[0].strip()
                translated_flag, timestamp, domain, content_num, link_num, keyword_flag = line.split(' ')
                translated_flag = int(translated_flag)
                timestamp = float(timestamp)
                content_num = int(content_num)
                link_num = int(link_num)
                keyword_flag = (keyword_flag != "False")
        except:
            print ("empty meta")
            write_to_accessed_line(accessed_file_list, source_html, domain, status=4)
            continue

        if (keyword_flag == 0):
            print ("no keyword")
            write_to_accessed_line(accessed_file_list, source_html, domain, status=1)
            continue

        if (float(content_num+1)/float(content_num+link_num+1)<0.2) and ('/my/' not in source_html) :
            print ("too many links")
            write_to_accessed_line(accessed_file_list, source_html, domain, status=2)
            continue
        
        with open(tmp_content, "r") as f:
            input_text = f.read()
        output_text, translate_status = translate_text(input_text, from_lang, to_lang, URL, KEY, NAME, consumer)

        if (translate_status != 0):
            print ("file too long")
            write_to_accessed_line(accessed_file_list, source_html, domain, status=7)
            continue

        status = save_result(output_text, target_txt)

        if (status == 6):
            print ("no permission")
            write_to_accessed_line(accessed_file_list, source_html, domain, status=6)
            continue

        if (status == 8):
            print ("elastic search error")
            write_to_accessed_line(accessed_file_list, source_html, domain, status=8)
            continue

        write_to_accessed_line(accessed_file_list, source_html, domain, status=0)

        # write log
        save_log(log_folder, target_html)
        print ("Translation output: {}".format(target_txt))

# initialize
def translation_account_init(account_index):
    NAME, KEY, SECRET = account_list[account_index] 
    consumer = OAuth1(KEY, SECRET)
    if (account_index == 0):
        process_domains = domains[0:int(len(domains)/2)]
    elif (account_index == 1):
        process_domains = domains[int(len(domains)/2):]
    elif (account_index == 2):
        process_domains = domains[0:int(len(domains)/2)]
    elif (account_index == 3):
        process_domains = domains[int(len(domains)/2):]
    return process_domains, NAME, KEY, consumer

def elastic_search_init(to_lang):
    """
    initialize elastic search
    """
    es_host = 'basil505'
    es_port = 9200
    es_ja_index = 'covid19-pages-ja'
    es_en_index = 'covid19-pages-en'
    es_ja_doc_index = 'covid19-docs-ja'
    es_en_doc_index = 'covid19-docs-en'
    html_dir = "/mnt/hinoki/share/covid19/html"
    es_ja_importer = ElasticSearchImporter(es_host, es_port, html_dir, 'ja', logger=None)
    es_en_importer = ElasticSearchImporter(es_host, es_port, html_dir, 'en', logger=None)
    if (to_lang == 'ja'):
        es_importer = es_ja_importer
        es_index = es_ja_index
        es_doc_index = es_ja_doc_index
    elif (to_lang == 'en'):
        es_importer = es_en_importer
        es_index = es_en_index
        es_doc_index = es_en_doc_index
    return es_importer, es_index, es_doc_index

def to_lang_specific_params_init(to_lang):
    """
    initialize other parameters
    """
    if (to_lang == 'ja'):
        accessed_file_list = ja_accessed_file_list
        URLs = Ja_URLs
        loc_phrase = "ja_translated"
        log_file_base_lang = "new-translated-files"
    elif (to_lang == 'en'):
        accessed_file_list = en_accessed_file_list
        URLs = En_URLs
        loc_phrase = "en_translated"
        log_file_base_lang = "new-translated-files-en"
    return accessed_file_list, URLs, loc_phrase, log_file_base_lang

if (__name__ == "__main__"):
    # initialization 
    process_domains, NAME, KEY, consumer = translation_account_init(account_index)
    es_importer, es_index, es_doc_index = elastic_search_init(to_lang)
    accessed_file_list, URLs, loc_phrase, log_file_base_lang = to_lang_specific_params_init(to_lang)
    # main loop
    print (f"Begin translation from domains:{process_domains} to lang:{to_lang}")
    while (1):
        for domain in process_domains:
            unaccessed_tmp_content = get_unaccessed_tmp_with_domain_site_limit(parent_folder, domain, limit_num=10)
            print (domain, len(unaccessed_tmp_content))
            if (len(unaccessed_tmp_content)>0):
                year, month, day, hour, minute=my_get_time()
                print ("Date: {}/{} {}:{}".format(month, day, hour, minute))
                print (domain, len(unaccessed_tmp_content))
                process_all(unaccessed_tmp_content)
        time.sleep(1)
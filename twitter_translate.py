from utils import *
from confidental_info import account_list 

# input arguments
account_index = int(sys.argv[1])
to_lang = sys.argv[2]

parent_folder = '/mnt/hinoki/share/covid19/twitter/html'
tweet_folder = "/mnt/hinoki/share/covid19/run/new-html-files"
log_folder = '/mnt/hinoki/share/covid19/run/new-translated-files'
extract_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/extract_tweet_list.txt'
en_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/en_tweet_trans_list.txt'
ja_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/ja_tweet_trans_list.txt'

def get_extracted_files(filename):
    names = [] 
    with open(filename, "r") as f:
        lines = f.readlines()
    for line in lines:
        line = line.strip()
        names.append(line)
    return names

def get_translated_files(filename):
    names = {}
    with open(filename, "r") as f:
        lines = f.readlines()
    for line in lines:
        line = line.strip()
        try:
            name = line.split()[0]
        except:
            continue
        names[name]=1
    return names

def save_result(output_text, translated_txt_file):
    status = 0
    translated_html_file = str(Path(translated_txt_file).with_suffix(".html"))
    output_html = text_to_html(output_text)

    with open(translated_txt_file, "w") as f:
        f.write(output_text)
    with open(translated_html_file, "w") as f:
        f.write(output_html)

    #elastic search
    try:
        res = es_importer.update_record(translated_txt_file, index=es_index, is_data_stream=True)
    except:
        print ("Twitter translation Error: elastic")
        status = 3
    return status

def save_log(log_folder, target_html):
    year, month, day, hour, minute = my_get_time()
    minute = 0
    log_file_base = "{}-{:04d}-{:02d}-{:02d}-{:02d}-{:02d}.txt".format(log_file_base_lang, year, month, day, hour, minute)
    log_file  = "{}/{}".format(log_folder, log_file_base)
    with open(log_file, "a+") as f:
        f.write(target_html.strip()+'\n')

def translate_file(name):
    status = 0 # 0: normal 1: empty text 2: cannot make folder 
    # parameter names
    orig_html = name
    orig_json = str(Path(orig_html).with_suffix(".json"))
    domain = orig_json.replace(parent_folder, "").split('/')[0] 
    translated_html = orig_html.replace("/orig/", "/{}_translated/".format(to_lang)) 
    translated_txt = str(Path(translated_html).with_suffix(".txt")) # replace with translated folder

    # make folder
    translated_folder = os.path.dirname(translated_html)
    try:
        os.makedirs(translated_folder, exist_ok=True)
    except:
        status = 2
        write_to_accessed_line(accessed_file_list, orig_html, to_lang, status)
        return

    # extract text
    line = open(orig_json, "r").readline()
    json_line = json.loads(line.strip())
    lang = json_line['lang'] # only "en" and "ja"
    text = get_text_from_html(orig_html)
    print ("Input file: {}".format(name))
    print ("Input text: {}".format(text))
    if (text == ""):
        write_to_accessed_line(accessed_file_list, orig_html, to_lang, status=1)
        return

    # translate
    text = save_hashtag_url(text)
    start_newlines, end_newlines = get_start_end_newlines(text)
    translated_text = translate_part(text, lang, to_lang, URLs[lang], KEY, NAME, consumer)
    translated_text = start_newlines + translated_text + end_newlines

    # save result
    if (save_result(translated_text, translated_txt) !=0):
        status = 3
    write_to_accessed_line(accessed_file_list, orig_html, to_lang, status)
    save_log(log_folder, translated_html)
    print ("Output: {}".format(translated_html))

def translation_account_init(account_index):
    NAME, KEY, SECRET = account_list[account_index]
    consumer = OAuth1(KEY, SECRET)
    return NAME, KEY, consumer

def to_lang_specific_params_init(to_lang):
    if (to_lang == 'ja'):
        accessed_file_list = ja_accessed_file_list
        URLs = Ja_URLs
        log_file_base_lang = "new-twitter-translated-files"
    elif (to_lang == 'en'):
        accessed_file_list = en_accessed_file_list
        URLs = En_URLs
        log_file_base_lang = "new-twitter-translated-files-{}".format(to_lang)
    loc_phrase = "{}_translated".format(to_lang)
    return accessed_file_list, URLs, loc_phrase, log_file_base_lang

def elastic_search_init(to_lang):
    """
    initialize elastic search
    """
    es_host = 'basil505'
    es_port = 9200
    es_ja_index = 'covid19-tweets-ja'
    es_en_index = 'covid19-tweets-en'
    es_index = ''
    html_dir = "/mnt/hinoki/share/covid19/twitter/html"
    es_ja_importer = ElasticSearchTwitterImporter(es_host, es_port, html_dir, 'ja', logger=None)
    es_en_importer = ElasticSearchTwitterImporter(es_host, es_port, html_dir, 'en', logger=None)
    if (to_lang == 'ja'):
        es_importer = es_ja_importer
        es_index = es_ja_index
    elif (to_lang == 'en'):
        es_importer = es_en_importer
        es_index = es_en_index
    return es_importer, es_index

if (__name__ == "__main__"):
    # initialization 
    NAME, KEY, consumer = translation_account_init(account_index)
    accessed_file_list, URLs, loc_phrase, log_file_base_lang = to_lang_specific_params_init(to_lang)
    es_importer, es_index = elastic_search_init(to_lang)
    # main loop
    while (1):
        extracted_files = get_extracted_files(extract_accessed_file_list) # dict
        translated_files = get_translated_files(accessed_file_list) # list
        under_translated_files = list_item_not_in_dict(extracted_files, translated_files)
        print ("There are {} tweets under translation.".format(len(under_translated_files)))
        if (len(under_translated_files)==0):
            time.sleep(10)
            continue
        under_translated_files = under_translated_files[-1000:]+under_translated_files[:1000] # get the last and front 1000 tweets
        print ("Tweet translation begins -----------------------------------")
        print ("Number of under_translated files: {}".format(len(under_translated_files)))
        for name in under_translated_files:
            translate_file(name)
            #except:
            #    error_message = f'Tweet translation error: {name}'
            #    write_to_error_log_file(error_message, error_log_file)
            #    write_to_accessed_line(accessed_file_list, name, to_lang, status=1)
        time.sleep(10)
from utils import *

parent_folder = '/mnt/hinoki/share/covid19/twitter/html'
tweet_folder = "/mnt/hinoki/share/covid19/run/new-html-files"
extract_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/extract_tweet_list.txt'

def write_to_access(names):
    with open(extract_accessed_file_list, "a+") as f:
        for name in names:
            if (name[:4]=='/mnt'):
                    f.write(name.strip()+'\n')

def get_accessed_files(extract_accessed_file_list):
    all_names = {} 
    with open(extract_accessed_file_list, "r") as f:
        lines = f.readlines()
    for line in lines:
        line = line.strip()
        all_names[line]=1
    return all_names

def read_all_input_htmls_with_time_limit(parent_folder, day_limit=7):
    """
        return htmls within day_limit
    """
    all_names = []
    for i in range(0, day_limit):
        dt = datetime.now() - timedelta(i)
        year, month, day = dt.year, dt.month, dt.day
        log_files_pattern = "{}/new-twitter-html-files*{:4d}-{:02d}-{:02d}*.txt".format(tweet_folder, year, month, day)
        file_names = glob.glob(log_files_pattern, recursive=True)
        for file_name in file_names:
            with open(file_name, "r") as f:
                for line in f.readlines():
                    all_names.append(line.strip())
    return all_names

while (1):
    extract_accessed_files = get_accessed_files(extract_accessed_file_list) # return dict
    all_names = read_all_input_htmls_with_time_limit(parent_folder, 10000)
    all_names = [name for name in all_names if (extract_accessed_files.get(name, 0)==0) and name[:4]=='/mnt']
    print ("For the extract tweet, got {} files:".format(len(all_names)))
    write_to_access(all_names)
    time.sleep(10)
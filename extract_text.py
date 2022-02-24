# usage here: https://github.com/Alir3z4/html2text/blob/master/docs/usage.md
from utils import *
import multiprocessing


parent_folder = '/mnt/hinoki/share/covid19/html'
html_folder = "/mnt/hinoki/share/covid19/run/new-html-files"
extract_accessed_file_list = '/mnt/hinoki/share/covid19/run/trans_log_song/extract_list.txt'

export_command = "export PATH=/orange/ubrew/data/bin:$PATH && export PERL5LIB=/home/song/usr/lib/perl:/home/song/usr/lib/perl/lib/perl5:/home/song/usr/lib/perl/lib/perl5/x86_64-linux-thread-multi:/home/song/perl5:/home/song/perl5/lib/perl5:/home/song/perl5/lib/perl5/site_perl/5.26.2:. && "
gene_block_command = "/home/song/git/WWW2sf/tool/html2sf.sh -T -D /home/song/git/detectblocks"
gene_block_command = export_command + gene_block_command

year_month_day_hour_pattern = re.compile(".*?/(\d\d\d\d)/(\d\d)/(\d\d)-(\d\d).*")

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

def read_all_input_htmls_with_domain(parent_folder, domain):
    """
        return list of (url, domain) of all htmls in the parent folder
    """
    search_path = "{}/{}/orig/**/*.html".format(parent_folder, domain)
    names_of_domain = glob.glob(search_path, recursive=True)
    names_of_domain_is_file = [name for name in names_of_domain if os.path.isfile(name)] 
    return names_of_domain_is_file

def read_all_input_htmls(parent_folder):
    """
        return list of (url, domain) of all htmls in the parent folder
    """
    all_names = []
    for domain in domains: 
        search_path = "{}/{}/orig/**/*.html".format(parent_folder, domain)
        names_of_domain = glob.glob(search_path, recursive=True)
        names_of_domain = [name for name in names_of_domain if os.path.isfile(name)] 
        for name in names_of_domain:
            all_names.append(name)
    return all_names

def read_all_input_htmls_with_folder(folder):
    """
        return list of (url, domain) of all htmls in the parent folder
    """
    all_names = []
    search_path = "{}/**/*.html".format(folder)
    names_of_domain = glob.glob(search_path, recursive=True)
    names_of_domain = [name for name in names_of_domain if os.path.isfile(name)] 
    for name in names_of_domain:
        all_names.append(name)
    return all_names

def read_all_input_htmls_with_domain_and_site_limit(parent_folder, domain, limit_num, day_limit=7) -> List[Tuple[str, str]]:
    """
        return htmls of domain within day_limit, maximum limit_num for each site
    """
    domain_pattern = "{}/{}".format(parent_folder, domain)
    site_pattern = re.compile("{}/{}/orig/(.*?)/.*".format(parent_folder, domain))

    all_names = []
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
                if (domain_pattern not in line):
                    continue
                site_res = re.match(site_pattern, line)
                if (site_res == None):
                    continue
                site = site_res.group(1)
                site_num[site] = site_num.get(site, 0) + 1
                if (site_num[site]<=limit_num):
                    all_names.append(line)
    return all_names

def write_to_accessed_file(file_name, name):
    with open(file_name, "a+") as f:
        f.write(name.strip()+'\n')

def gene_tmp(name, domain):
    tmp_name = name.replace("/{}/orig/".format(domain), "/{}/tmp/".format(domain))
    tmp_name_dir = os.path.dirname(tmp_name)
    if (os.path.exists(tmp_name_dir) == 0):
        os.makedirs(tmp_name_dir)

    block_name = str(Path(tmp_name).with_suffix(".block"))
    title_main_name = str(Path(tmp_name).with_suffix(".title_main"))
    meta_name = str(Path(tmp_name).with_suffix(".meta"))

    if (os.path.exists(meta_name) == 1):
        write_to_error_log_file(f"In extract_text, name = {name} error, meta_name already exists")
        write_to_accessed_file(extract_accessed_file_list, name)
        return

    if (domain=='jp'):
        command = "{} '{}' > '{}'".format(gene_block_command, name, block_name)
    elif (domain == 'cn'):
        command = "{} -f '{}' > '{}'".format(gene_block_command, name, block_name)
    else:
        command = "{} -E '{}' > '{}'".format(gene_block_command, name, block_name)

    try:
        os.system(command)
        result, content_num, link_num = extract_source_text_xml(block_name)
    except:
        print (f"In extract_text, name = {name} error, command = {command}")
        write_to_error_log_file(f"In extract_text, name = {name} error, command error")
        write_to_accessed_file(extract_accessed_file_list, name)
        return

    # title main
    if ('zusammengegencorona' in name):
        h1_text = first_h1_text(name) 
        with open(title_main_name, "w") as f:
            if (len(h1_text)>0):
                f.write(h1_text.strip()+'\n')
            f.write(result)
    else:
        with open(title_main_name, "w") as f:
            f.write(result)

    # meta info
    timestamp = os.path.getmtime(name)
    keyword_flag = contain_keyword(result)
    meta_info = "{} {} {} {} {} {}".format(0, timestamp, domain, content_num, link_num, keyword_flag)
    with open(meta_name, "w") as f:
        f.write(meta_info)
    write_to_accessed_file(extract_accessed_file_list, name)
    print (f"Extract text Output: {title_main_name}")

def process_domain(domain):
    print (f"Extract text of {domain}")
    all_names = read_all_input_htmls_with_domain_and_site_limit(parent_folder, domain, limit_num) # list of names
    unaccessed_file_list = list_item_not_in_dict(all_names, extract_accessed_files)
    print (f"{len(unaccessed_file_list)} htmls need to be extracted for domain {domain}")

    for i, name in enumerate(unaccessed_file_list):
        print (f"Extract text {name}")
        gene_tmp(name, domain)

limit_num = 20 
while (1):
    extract_accessed_files = get_accessed_files(extract_accessed_file_list) # dict
    jobs = []
    for domain in domains:
        p = multiprocessing.Process(target=process_domain, args=(domain,))
        jobs.append(p)
        p.start()
    # wait until all jobs done
    for p in jobs:
        p.join()

    time.sleep(10)

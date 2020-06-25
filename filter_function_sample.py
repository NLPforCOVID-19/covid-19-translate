import os

keywords = ["COVID", "covid", "肺炎", "コロナ", "corona", "Corona", "코로나"] # En, zh, ja, fr, ko

def contain_keyword(input_text):
    for keyword in keywords:
        if (keyword in input_text):
            return True
    return False

def filter(path_to_file):
    with open(path_to_file, "r") as f:
        input_text = f.read()
    contain_keyword_flag = contain_keyword(input_text)
    if (contain_keyword_flag==0):
        return 0 #"Not containing any keyword"
    else:
        return 1 #"Containing keyword(s)"

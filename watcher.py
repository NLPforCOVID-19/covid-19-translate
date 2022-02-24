import os
import sys
import glob
import time
import subprocess
from subprocess import check_output
from datetime import datetime, timedelta

#log_file = '/mnt/hinoki/share/covid-19-logs/errorlog_song'
log_file = '/mnt/hinoki/share/covid19/run/trans_log_song/watcher_log.txt'

import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate

TO_ADDRESS ="songskg6666@gmail.com"
FROM_ADDRESS = "covid19infotest@gmail.com"
MY_PASSWORD = "covid19test"
SUBJECT = 'Covid19_error_message'


def create_message(from_addr, to_addr, subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Date'] = formatdate()
    return msg

def send(from_addr, to_addrs, msg):
    smtpobj = smtplib.SMTP('smtp.gmail.com', 587)
    smtpobj.ehlo()
    smtpobj.starttls()
    smtpobj.ehlo()
    smtpobj.login(FROM_ADDRESS, MY_PASSWORD)
    smtpobj.sendmail(from_addr, to_addrs, msg.as_string())
    smtpobj.close()

def my_get_time():
    dt = datetime.now()
    year, month, day, hour, minute = dt.year, dt.month, dt.day, dt.hour, dt.minute
    return year, month, day, hour, minute

def get_pid(name):
    try:
        sres = check_output(["pgrep","-f",name]).split()
    except:
        return []
    res = [int(i) for i in sres]
    return res

def end_program():
    pids = get_pid("extract_text.py") + get_pid("translate.py") + get_pid("extract_tweet.py") + get_pid("twitter_translate.py") + get_pid("launch.py") + get_pid("sentiment_classifier.py")
    print (pids)
    for pid in pids:
        os.system("kill -9 {}".format(pid))

def start_program():
    print(1)
    subprocess.Popen('bash twitter_translate.sh', shell=True)
    print(2)
    subprocess.Popen('bash translate.sh', shell=True)
    print(3)
    subprocess.Popen('bash extract_text.sh', shell=True)
    print(4)
    subprocess.Popen('bash extract_tweet.sh', shell=True)
    print(5)
    subprocess.Popen('bash sentiment.sh', shell=True)
    print(6)
    subprocess.Popen('bash topic_classifier.sh', shell=True)

def restart():
    end_program()
    time.sleep(3)
    start_program()


def get_status_line():
    line = ''
    if len(get_pid("extract_text.py"))==0:
        line += "Article extract error\n"
    if len(get_pid("translate.py"))==0:
        line += "Article translate error\n"
    if len(get_pid("extract_tweet.py"))==0:
        line += "Tweet extract error\n"
    if len(get_pid("twitter_translate.py"))==0:
        line += "Tweet translate error\n"
    if len(get_pid("launch.py"))==0:
        line += "Topic classification error\n"
    if len(get_pid("sentiment_classifier.py"))==0:
        line += "Sentiment classification error\n"
    if (line == ''):
        line = 'No error\n'
    else:
        msg = create_message(FROM_ADDRESS, TO_ADDRESS, SUBJECT, line)
        #send(FROM_ADDRESS, TO_ADDRESS, msg)
    year, month, day, hour, minute=my_get_time()
    date_info = "Date: {}/{} {}:{}\n".format(month, day, hour, minute)

    line = date_info + line
    return line

def write_status_to_log():
    status_line = get_status_line()
    with open(log_file, "a+") as f:
        f.write(status_line.strip()+'\n')

#end_program()
#exit()
watch_freq = 8
sleep_period = 7200 
while (1):
    restart()
    for i in range(watch_freq):
        time.sleep(10)
        write_status_to_log()
        time.sleep(sleep_period)

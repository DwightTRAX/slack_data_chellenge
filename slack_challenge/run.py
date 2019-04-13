import os
import wget
import tarfile
from db import DB
from parser import Parser

class Process(object):
    def __init__(self):
        self.db = DB()
        self.message_response = {}
        self.db.create_tables()

    def dataset_ready(self):
        if os.path.isdir("enron_with_categories"):
            print "You already have or download the data set"
            return True # check condition if self.dataset_ready()
        url = "http://bailando.sims.berkeley.edu/enron/enron_with_categories.tar.gz"
        dataset = wget.download(url)
        tar = tarfile.open(dataset)
        tar.extractall()
        tar.close()
        print "data set is ready"
        return (os.path.isdir("enron_with_categories"))

    def parse_emails(self):
        each_txt_files = set()
        list_of_value_dicts = []
        pp = Parser()
        for root, dirs, files in os.walk(os.getcwd() + "/enron_with_categories"):
            for each_file in files:
                if each_file.endswith(".txt"):
                    if each_file in each_txt_files:
                        continue
                    each_txt_files.add(each_file)
                    f = os.path.join(root, each_file)
                    row = pp.extract_values(open(f, 'r').readlines())
                    list_of_value_dicts.append(pp.extract_values(open(f, 'r').readlines()))
        return list_of_value_dicts

    def insert_data_to_table(self, list_of_value_dicts):

        for row in list_of_value_dicts:
            email_row = row.get('message_id'), row.get('from'), row.get('subject'), self.db.convert_date_format(row.get('date')), self.get_label(len(row.get('to')),len(row.get('cc')),len(row.get('bcc'))), row.get('sub_md5')
            email_rows = [email_row]
            recipient_rows = []
            for to in row.get('to'):
                recipient_row = row.get('message_id'), row.get('from'), to, row.get('subject'), row.get('sub_md5'), self.db.convert_date_format(row.get('date')), 1, 0, 0
                recipient_rows.append(recipient_row)

            for cc in row.get('cc'):
                recipient_row = row.get('message_id'), row.get('from'), cc, row.get('subject'), row.get('sub_md5'), self.db.convert_date_format(row.get('date')), 0, 1, 0
                recipient_rows.append(recipient_row)

            for bcc in row.get('bcc'):
                recipient_row = row.get('message_id'), row.get('from'), bcc, row.get('subject'), row.get('sub_md5'), self.db.convert_date_format(row.get('date')), 0, 0, 1
                recipient_rows.append(recipient_row)
            self.db.insert('email', email_rows)
            self.db.insert('recipient', recipient_rows)
        return True

    def get_label(self, to, cc, bcc):
        if to == 0 and cc == 0 and bcc == 0: # no to, no cc, no bcc recipients
            return 'no recipient'
        else:
            return 'have recipient'

    def daily_emails_receive(self):
        """
        How many emails did each person receive each day?
        """
        query = """SELECT recipient, count(distinct(message_id)) AS cnt, DATE(email_date)
                   FROM recipient GROUP BY recipient,DATE(email_date) ORDER BY cnt DESC;"""
        print "Q1 The following is a record of number of emails received by each person daily"
        print "You can also check the file output.txt"
        print "EMAIL", '|', "COUNT", '|' ,"DATE"
        print "================================="

        with open('output_question_one.txt', 'w') as f:
            f.write("EMAIL | COUNT | DATE_TIME\n")
            for email_addr, count, date in self.db.run_query(query):
                print email_addr, '|', count, '|' , unicode(date)
                f.write(email_addr + ' | ' + str(count) + ' | ' + str(unicode(date)) + '\n')
            f.write("============THIS IS THE END===============")

        print "================================="


    def find_top_broad_direct(self):

        """
        Find the person who received the largest number of direct emails.
        """
        query = """SELECT recipient, count(message_id) AS cnt FROM recipient WHERE message_id IN
                   (SELECT message_id AS count_r FROM recipient GROUP BY message_id
                    HAVING count(distinct(recipient)) = 1) GROUP BY recipient ORDER BY cnt DESC;"""
        print "Q2 (A) The following person RECIEVED the largest number of DIRECT emails:---"
        email_addr, count = self.db.run_query(query, 1)
        print unicode(email_addr), count
        print "================================="

        """
        Find the person who sent the largest number of broadcast emails.
        """

        query = """SELECT sender, count(message_id) AS count_email
                   FROM (SELECT message_id, sender, count(distinct(recipient)) AS count_r
                   FROM recipient GROUP BY message_id, sender HAVING count_r > 1) as t1
                   GROUP BY sender ORDER BY count_email DESC;"""

        print "Q2 (B) The following person SENT the largest number of BROADCASTING emails"
        email_addr, count = self.db.run_query(query, 1)
        print unicode(email_addr), count
        print "================================="

    def fast_five_reply_email(self):
        """
        Find the five emails reply/original rank by the time difference.
        """
        query = """SELECT distinct(rone.message_id), rone.subject, rone.sender, rone.recipient, rtwo.message_id, rtwo.subject, rtwo.sender, rtwo.recipient, TIMESTAMPDIFF(SECOND, rone.email_date , rtwo.email_date)
                   FROM recipient rone INNER JOIN recipient rtwo WHERE rone.sub_md5 IS NOT NULL AND rtwo.sub_md5 IS NOT NULL
                   AND rone.sub_md5 = rtwo.sub_md5 AND rone.message_id != rtwo.message_id
                   AND TIMESTAMPDIFF(SECOND, rone.email_date , rtwo.email_date) >= 0 AND rone.sender = rtwo.recipient
                   AND rtwo.sender = rone.recipient AND rone.sender != rone.recipient
                   ORDER BY TIMESTAMPDIFF(SECOND, rone.email_date , rtwo.email_date) ASC limit 5;"""

        print "Q3  The following 5 email/message_id had fastest response time(measured in SECONDS):---"
        print "REPLY_ID", "|", "REPLY_SUBJECT", "|", "REPLY_SENDER", "|", "REPLY_RECIPIENT", "|", "ORIGINAL_ID", "|", "ORIGINAL_SUBJECT", "|", "ORIGINAL_SENDER", "|", "ORIGINAL_RECIPIENT", "|", "TIME_DIFFERENCE(in seconds)"
        with open('output_question_three.txt', 'w') as f:
            f.write("REPLY_ID | REPLY_SUBJECT | REPLY_SENDER | REPLY_RECIPIENT | ORIGINAL_ID | ORIGINAL_SUBJECT | ORIGINAL_SENDER | ORIGINAL_RECIPIENT | TIME_DIFFERENCE(in seconds)\n")
            for re_id, re_sub, re_sender, re_recipient, ori_id, ori_sub, ori_sender, ori_recipient, time_diff in self.db.run_query(query, 5):
                print re_id, "|", re_sub, "|", re_sender, "|", re_recipient, "|",ori_id, "|", ori_sub, "|", ori_sender, "|", ori_recipient, "|",time_diff
                f.write(re_id + " | " + re_sub + " | " + re_sender + " | " + re_recipient + " | " + ori_id + " | " + ori_sub + " | " + ori_sender + " | " + ori_recipient + " | " + str(time_diff) + "\n")

        print "================================="

    def execute(self):
        if self.dataset_ready(): # check data ready
            list_of_value_dicts = self.parse_emails()
            if list_of_value_dicts: # check txt file parsed successfully and it exitsts
                if self.insert_data_to_table(list_of_value_dicts):
                    return True # check parsed data insert into DB successfully

if __name__ == "__main__":
    task_run = Process()
    if task_run.execute(): # check whether data ready and insert data into DB successfully
        #For each question, run the query
        task_run.daily_emails_receive()
        task_run.find_top_broad_direct()
        task_run.fast_five_reply_email()

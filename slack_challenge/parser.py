import re
import hashlib

class Parser(object):
    def __init__(self):
        self.name = "parser"

    def extract_values(self, email):
        msg_dict = {}
        to_addr = []
        cc_addr = []
        bcc_addr = []
        from_addr = None
        msg_id = None
        subject = None
        dt = None
        indicator = ""


        for line in email:
            line = line.replace("\r", "").replace("\n", "").replace("\t", "").strip()
            if "X-" in line:
                break
            elif "Message-ID:" in line: # assume Message-ID is only one line
                indicator = "Message-ID:"
                msg_id = line.replace("Message-ID:", "").strip()
            elif "Date:" in line:
                indicator = "Date:"
                dt = line.replace("Date:", "").strip()
            elif "From:" in line:
                indicator = "From:"
                from_addr = line.replace("From:", "").strip()
            elif "To:" in line:
                indicator = "To:"
                t = [i.strip() for i in line.replace("To:", "").strip().split(",") if len(i) > 1]
                to_addr.extend(t)
            elif "Cc:" in line:
                indicator = "Cc:"
                t = [i.strip() for i in line.replace("Cc:", "").strip().split(",") if len(i) > 1]
                cc_addr.extend(t)
            elif "Bcc:" in line:
                indicator = "Bcc:"
                t = [i.strip() for i in line.replace("Bcc:", "").strip().split(",") if len(i) > 1]
                bcc_addr.extend(t)
            elif "Subject:" in line:
                indicator = "Subject:"
                subject = line.replace("Subject:", "").strip()
            elif "Mime-Version:" in line or "Content-Type:" in line or "Content-Transfer-Encoding:" in line:
                indicator = ""
            else:  # handle mutiple lines if subject,to,cc,bcc have more than one line
                if indicator == "To:":
                    t = [i.strip() for i in line.strip().split(",") if len(i) > 1]
                    to_addr.extend(t)
                if indicator == 'Cc:':
                    t = [i.strip() for i in line.strip().split(",") if len(i) > 1]
                    cc_addr.extend(t)
                if indicator == 'Bcc:':
                    t = [i.strip() for i in line.strip().split(",") if len(i) > 1]
                    bcc_addr.extend(t)
                if indicator == "Subject:":
                    subject = subject + line
        msg_dict['message_id'] = msg_id if msg_id else ""
        msg_dict['date'] = dt
        msg_dict['from'] = from_addr.strip() if from_addr else ""
        msg_dict['to'] = [i.strip() for i in to_addr if '@' in i]
        msg_dict['cc'] = [i.strip() for i in cc_addr if '@' in i]
        msg_dict['bcc'] = [i.strip() for i in bcc_addr if '@' in i]
        msg_dict['subject'] = subject.strip() if subject else ""
        msg_dict['sub_md5'] = hashlib.md5(subject.lower().replace("re:", "").strip()).hexdigest() if subject else None

        return msg_dict

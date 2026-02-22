from datetime import datetime
import os
import shutil
import traceback
from sqlcipher3 import dbapi2 as sqlcipher


class bcolors:
    INFO = "\033[90m"
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


TIME_COLUMN = '"40050"'  # msgTime

DB_INFO = {
    "nt_msg.db": {
        "c2c_msg_table": ['"40002"', '"40050"'],  # msgRandom  # msgTime
        "group_msg_table": ['"40002"', '"40050"'],  # msgRandom  # msgTime
        "group_at_me_msg": ['"40001"', '"40050"'],  # msgId  # msgTime
        "dataline_msg_table": ['"40002"', '"40050"'],  # msgRandom  # msgTime
        "discuss_msg_table": ['"40002"', '"40050"'],  # msgRandom  # msgTime
        # "nt_kv_storage_table": ['"48901"'],  # key
        "pai_yi_pai_msg_id_table": ['"48901"'],  # key
        # "nt_uid_mapping_table": ['"48902"', '"48912"', '"1002"'],  # uid  # uid2  # old_uid
        # "recent_contact_v3_table": ["40021"],  # uid
    },
    "files_in_chat.db": {"files_in_chat_table": ['"82300"', '"40050"']},  # msgRandom  # msgTime
    "rich_media.db": {"file_table": ['"45503"']},  # file_id
    "buddy_msg_fts.db": {"buddy_msg_fts": ['"40001"', '"40050"']},  # msgId  # msgTime
    "data_line_msg_fts.db": {"data_line_msg_fts": ['"40001"', '"40050"']},  # msgId  # msgTime
    "discuss_msg_fts.db": {"discuss_msg_fts": ['"40001"', '"40050"']},  # msgId  # msgTime
    "group_msg_fts.db": {"group_msg_fts": ['"40001"', '"40050"']},  # msgId  # msgTime
}


def get_documents_folder():
    # Source - https://stackoverflow.com/a/30924555
    # Posted by axil, modified by community. See post 'Timeline' for change history
    # Retrieved 2026-02-13, License - CC BY-SA 3.0

    import ctypes.wintypes

    CSIDL_PERSONAL = 5  # My Documents
    SHGFP_TYPE_CURRENT = 0  # Get current, not default value

    buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
    ctypes.windll.shell32.SHGetFolderPathW(None, CSIDL_PERSONAL, None, SHGFP_TYPE_CURRENT, buf)

    return buf.value


def get_ntqq_base_path():
    base = os.path.join(get_documents_folder(), "Tencent Files")
    arr = []
    for number in os.listdir(base):
        if number.isdigit():
            arr.append(number)
    if len(arr) == 0:
        raise Exception("No account found in Tencent Files")
    elif len(arr) > 1:
        for i, number in enumerate(arr):
            print(f"{i}: {number}")
        index = int(input("Multiple qq accounts found. Please select the index of the folder to use: "))
        assert 0 <= index < len(arr), "Invalid index"
        return os.path.join(base, arr[index])
    else:
        print(f"Found account {arr[0]} in Tencent Files")
        return os.path.join(base, arr[0])


def get_file_basename(filename):
    s = filename
    if "." in s:
        s = os.path.splitext(os.path.basename(s))[0]
    # 38-character UUID in the form of {xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}
    if len(s) == 38 and s[0] == "{" and s[-1] == "}" and s[1:37].count("-") == 4:
        s = s[1:-1].replace("-", "")
    return s.lower()


def get_rich_media_id(file_id):
    return file_id.replace("/", "_")


def input_time(prompt):
    time_str = input(prompt)
    time = None
    try:
        if time_str != "":
            time = int(datetime.strptime(time_str, "%Y-%m-%d").timestamp())
    except Exception as e:
        print("Invalid date format!")
        raise e
    return time


def decrypt(dbpath, dbfile, key):
    if os.path.exists(dbfile):
        print(f"{bcolors.WARNING}{dbfile} already exists, skipping decryption.{bcolors.ENDC}")
        return

    tempfile = dbfile.replace(".db", ".clean.db")
    with open(dbpath, "rb") as f:
        f.seek(1024)  # 跳过前1024字节
        with open(tempfile, "wb") as tempf:
            tempf.write(f.read())  # 将剩余部分写入临时文件

    # create new plaintext database
    # db2 = sqlite3.connect(dbfile)  # type: ignore
    # db2.commit()
    # db2.close()

    conn = sqlcipher.connect(tempfile)  # type: ignore
    try:
        conn.execute(f"PRAGMA key = '{key}';")
        # db.execute(f"PRAGMA cipher_page_size = 4096;")
        conn.execute(f"PRAGMA kdf_iter = 4000;")
        conn.execute(f"PRAGMA cipher_hmac_algorithm = HMAC_SHA1;")
        conn.execute(f"PRAGMA cipher_default_kdf_algorithm = PBKDF2_HMAC_SHA512;")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA journal_mode=OFF")
        # db.execute(f"PRAGMA cipher = 'aes-256-cbc';")
        conn.execute(f"ATTACH DATABASE '{dbfile}' AS plaintext KEY '';")
        conn.execute(f"SELECT sqlcipher_export('plaintext');")
        conn.execute(f"DETACH DATABASE plaintext;")
        conn.commit()
        conn.close()
    except Exception as e:
        conn.close()
        if os.path.exists(tempfile):
            os.remove(tempfile)
        if os.path.exists(dbfile):
            os.remove(dbfile)
        traceback.print_exc()
        raise e

    # print tables
    # conn = sqlite3.connect(dbfile)  # type: ignore
    # cursor = conn.cursor()
    # cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    # tables = cursor.fetchall()
    # print(f"Tables in {dbfile}: {[table[0] for table in tables]}")
    # conn.close()

    os.remove(tempfile)

    filesize = os.path.getsize(dbfile)
    print(f"{bcolors.OKGREEN}Decrypted {dbfile}, size: {filesize / (1024**2):.2f} MB{bcolors.ENDC}")


def encrypt(dbpath, dbfile, key):
    print(f"{bcolors.INFO}Encrypting {dbfile}...{bcolors.ENDC}")

    backup_path = dbpath.replace(".db", ".backup.db")

    assert os.path.exists(dbpath), f"{dbpath} does not exist!"
    if os.path.exists(backup_path):
        print(f"{bcolors.FAIL}{backup_path} already exists! Please delete backup file before proceeding.{bcolors.ENDC}")
        exit(1)

    shutil.copy(dbpath, backup_path)

    tempfile = dbfile.replace(".db", ".encrypt.db")
    if os.path.exists(tempfile):
        os.remove(tempfile)

    conn = sqlcipher.connect(dbfile)  # type: ignore
    conn.execute(f"ATTACH DATABASE '{tempfile}' AS encrypted KEY '{key}';")
    conn.execute(f"PRAGMA encrypted.kdf_iter = 4000;")
    conn.execute(f"PRAGMA encrypted.cipher_hmac_algorithm = HMAC_SHA1;")
    conn.execute(f"PRAGMA encrypted.cipher_default_kdf_algorithm = PBKDF2_HMAC_SHA512;")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute(f"SELECT sqlcipher_export('encrypted');")
    conn.execute(f"DETACH DATABASE encrypted;")
    conn.commit()
    conn.close()

    header = None
    with open(dbpath, "rb") as f:
        header = f.read(1024)
    with open(dbpath, "wb") as f:
        f.write(header)
        with open(tempfile, "rb") as tempf:
            shutil.copyfileobj(tempf, f)

    if os.path.exists(tempfile):
        os.remove(tempfile)

    print(
        f"{bcolors.OKGREEN}Encrypted {dbfile} successfully!\nBackup of original database is saved as {backup_path}{bcolors.ENDC}"
    )

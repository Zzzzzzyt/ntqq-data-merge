import os
from sqlcipher3 import dbapi2 as sqlcipher
import sqlite3


def decrypt(dbpath, dbfile, key):
    if os.path.exists(dbfile):
        print(f"{dbfile} already exists, skipping decryption.")
        return

    with open(dbpath, "rb") as f:
        tempfile = dbfile.replace(".db", ".clean.db")
        f.seek(1024)  # 跳过前1024字节
        with open(tempfile, "wb") as tempf:
            tempf.write(f.read())  # 将剩余部分写入临时文件

    # create new plaintext database
    # db2 = sqlite3.connect(dbfile)  # type: ignore
    # db2.commit()
    # db2.close()

    db = sqlcipher.connect(tempfile)  # type: ignore
    db.execute(f"PRAGMA key = '{key}';")
    # db.execute(f"PRAGMA cipher_page_size = 4096;")
    db.execute(f"PRAGMA kdf_iter = 4000;")
    db.execute(f"PRAGMA cipher_hmac_algorithm = HMAC_SHA1;")
    db.execute(f"PRAGMA cipher_default_kdf_algorithm = PBKDF2_HMAC_SHA512;")
    # db.execute(f"PRAGMA cipher = 'aes-256-cbc';")
    db.execute(f"ATTACH DATABASE '{dbfile}' AS plaintext KEY '';")
    db.execute(f"SELECT sqlcipher_export('plaintext');")
    db.execute(f"DETACH DATABASE plaintext;")
    db.commit()
    db.close()

    # print tables
    # conn = sqlite3.connect(dbfile)  # type: ignore
    # cursor = conn.cursor()
    # cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    # tables = cursor.fetchall()
    # print(f"Tables in {dbfile}: {[table[0] for table in tables]}")
    # conn.close()

    os.remove(tempfile)

    filesize=os.path.getsize(dbfile)
    print(f"Decrypted {dbfile}, size: {filesize / (1024**3):.2f} GB")


dbs = {
    "nt_msg.db": {
        "tables": {
            "c2c_msg_table",
            # "c2c_msg_flow_table",
            "group_msg_table",
            # "group_msg_flow_table",
            "group_at_me_msg",
            "dataline_msg_table",
            # "dataline_flow_table",
            # "c2c_temp_msg_table",
            # "c2c_temp_msg_flow_table",
            "discuss_msg_table",
            # "discuss_msg_flow_table",
            # "service_assistant_msg_table",
            # "nt_kv_storage_table",
            # "msg_unread_info_table",
            # "eventflow_seq_storage_table",
            # "draft_storage_table_v1",
            # "hidden_session_storage_table_v1",
            # "them_module_storage_table_v1",
            # "recent_contact_delete_storage",
            # "recent_contact_top_table",
            "pai_yi_pai_msg_id_table",
            # "game_msg_config_table",
            # "service_assistant_contact",
            # "ark_to_markdown_config_table",
            "nt_uid_mapping_table",
            # "sqlite_sequence",
            "recent_contact_v3_table",
            # "at_me_relay_history",
            # "search_history",
            # "kv_tofu_msg_table",
            # "msg_backup_storage_table",
        }
    },
    "files_in_chat.db": {"tables": {"files_in_chat_table"}},
    "rich_media.db": {"tables": {"file_table"}},
}

if __name__ == "__main__":
    from util import get_ntqq_base_path

    device_name = input("Device name: ")

    base_path = get_ntqq_base_path()
    print("You may get the key with https://github.com/artiga033/ntdb_unwrap")
    key = input("key: ")

    output_path = f"{device_name}_export/nt_db/"
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for dbname, dbinfo in dbs.items():
        print(f"Decrypting {dbname}...")
        dbpath = os.path.join(base_path, "nt_qq/nt_db/", dbname)
        dbfile = os.path.join(output_path, dbname)
        decrypt(dbpath, dbfile, key)

import os
from sqlcipher3 import dbapi2 as sqlcipher

from util import DB_INFO, bcolors


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


if __name__ == "__main__":
    from util import get_ntqq_base_path

    device_name = input(f"{bcolors.OKBLUE}Device name: {bcolors.ENDC}")

    base_path = get_ntqq_base_path()
    print("You may get the key with https://github.com/artiga033/ntdb_unwrap")
    key = input(f"{bcolors.OKBLUE}key: {bcolors.ENDC}")

    output_path = f"{device_name}_export/nt_db/"
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for dbname, dbinfo in DB_INFO.items():
        print(f"{bcolors.INFO}Decrypting {dbname}...{bcolors.ENDC}")
        dbpath = os.path.join(base_path, "nt_qq/nt_db/", dbname)
        dbfile = os.path.join(output_path, dbname)
        decrypt(dbpath, dbfile, key)

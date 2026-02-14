from sqlcipher3 import dbapi2 as sqlcipher
import os
import shutil

from util import DB_INFO, bcolors, get_ntqq_base_path


def encrypt(dbpath, dbfile, key):
    print(f"{bcolors.INFO}Encrypting {dbfile}...{bcolors.ENDC}")

    backup_path = dbpath.replace(".db", ".backup.db")

    assert os.path.exists(dbpath), f"{dbpath} does not exist!"
    if os.path.exists(backup_path):
        print(f"{bcolors.FAIL}{backup_path} already exists! Please delete backup file before proceeding.{bcolors.ENDC}")
        exit(1)

    shutil.copy(dbpath, backup_path)

    tempfile = dbfile.replace(".db", ".encrypt.db")

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

    print(
        f"{bcolors.OKGREEN}Encrypted {dbfile} successfully! Backup of original database is saved as {backup_path}{bcolors.ENDC}"
    )


if __name__ == "__main__":
    print(f"{bcolors.BOLD}{bcolors.WARNING}THIS SCRIPT WILL OVERWRITE YOUR LOCAL NTQQ DATA!!!{bcolors.ENDC}")
    print(f"{bcolors.BOLD}{bcolors.WARNING}USE AT YOUR OWN RISK!{bcolors.ENDC}")
    print(f"{bcolors.BOLD}{bcolors.WARNING}We will create backup files before overwriting.{bcolors.ENDC}")

    device_name = input(f"{bcolors.OKBLUE}Device name: {bcolors.ENDC}")
    base_path = get_ntqq_base_path()
    print("You may get the key with https://github.com/artiga033/ntdb_unwrap")
    key = input(f"{bcolors.OKBLUE}key: {bcolors.ENDC}")

    export_path = f"{device_name}_export/nt_db/"
    for dbname, dbinfo in DB_INFO.items():
        dbpath = os.path.join(base_path, "nt_qq/nt_db/", dbname)
        dbfile = os.path.join(export_path, dbname)
        encrypt(dbpath, dbfile, key)

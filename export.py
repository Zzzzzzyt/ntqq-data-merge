import os
import shutil
from sqlcipher3 import dbapi2 as sqlcipher

from util import DB_INFO, bcolors, decrypt, get_ntqq_base_path

if __name__ == "__main__":
    device_name = input(f"{bcolors.OKBLUE}Device name: {bcolors.ENDC}")

    base_path = get_ntqq_base_path()
    print("You may get the key with https://github.com/artiga033/ntdb_unwrap")
    key = input(f"{bcolors.OKBLUE}key: {bcolors.ENDC}")

    output_path = f"{device_name}_export/nt_db/"
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    ntdb_path = os.path.join(base_path, "nt_qq/nt_db/")
    for dbname in os.listdir(ntdb_path):
        if not dbname.endswith(".db"):
            continue
        if dbname.endswith(".backup.db"):
            continue
        print(f"{bcolors.INFO}Decrypting {dbname}...{bcolors.ENDC}")
        dbpath = os.path.join(ntdb_path, dbname)
        dbfile = os.path.join(output_path, dbname)
        try:
            decrypt(dbpath, dbfile, key)
        except Exception as e:
            print(f"{bcolors.FAIL}Failed to decrypt {dbname}: {e}{bcolors.ENDC}")
            print(f"{bcolors.WARNING}Copying original file for {dbname}...{bcolors.ENDC}")
            shutil.copy2(dbpath, dbfile)  # copy original file if decryption fails

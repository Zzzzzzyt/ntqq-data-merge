from sqlcipher3 import dbapi2 as sqlcipher
import os
import shutil

from util import DB_INFO, bcolors, encrypt, get_ntqq_base_path


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

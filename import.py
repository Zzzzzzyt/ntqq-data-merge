from sqlcipher3 import dbapi2 as sqlcipher
import os
import shutil

from util import DB_INFO, bcolors, encrypt, get_ntqq_base_path


if __name__ == "__main__":
    print(f"{bcolors.BOLD}{bcolors.WARNING}该脚本会覆盖你的本地NTQQ数据库！！！{bcolors.ENDC}")
    print(f"{bcolors.BOLD}{bcolors.WARNING}造成数据损坏等后果自负！！！{bcolors.ENDC}")
    print(f"{bcolors.BOLD}{bcolors.WARNING}覆盖之前，该脚本会在原位自动创建备份文件{bcolors.ENDC}")
    print(f"{bcolors.BOLD}{bcolors.WARNING}如果已有之前创建的备份文件则脚本会拒绝执行覆盖，请手动删除{bcolors.ENDC}")

    device_name = input(f"{bcolors.OKBLUE}设备名称: {bcolors.ENDC}")
    base_path = get_ntqq_base_path()
    print("你可以使用 https://github.com/artiga033/ntdb_unwrap 工具获得数据库密钥")
    key = input(f"{bcolors.OKBLUE}数据库密钥: {bcolors.ENDC}")

    export_path = f"{device_name}_export/nt_db/"
    for dbname, dbinfo in DB_INFO.items():
        dbpath = os.path.join(base_path, "nt_qq/nt_db/", dbname)
        dbfile = os.path.join(export_path, dbname)
        encrypt(dbpath, dbfile, key)

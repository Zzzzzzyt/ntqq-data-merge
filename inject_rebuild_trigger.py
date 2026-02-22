import sqlite3
import os

from util import DB_INFO, bcolors, get_ntqq_base_path, encrypt, decrypt

if __name__ == "__main__":
    print(
        f"{bcolors.BOLD}{bcolors.WARNING}该脚本将向你的NTQQ数据库注入重建FTS5触发器（或在已注入时，移除触发器）{bcolors.ENDC}"
    )
    print(
        f"{bcolors.BOLD}{bcolors.WARNING}在对应的聊天类型中发送该消息以触发重建（可能会使QQ卡顿一段时间）{bcolors.ENDC}"
    )
    print(f"__trigger_fts_rebuild__")
    print()

    base_path = get_ntqq_base_path()
    key = input(f"{bcolors.OKBLUE}数据库密钥: {bcolors.ENDC}")
    os.makedirs("trigger_temp", exist_ok=True)  # create temp folder for modified databases
    for file in os.listdir("trigger_temp"):
        os.remove(os.path.join("trigger_temp", file))  # clear temp folder

    for dbname in DB_INFO.keys():
        if dbname.endswith("fts.db"):
            print()

            dbpath = os.path.join(base_path, "nt_qq/nt_db/", dbname)
            temp_path = os.path.join("trigger_temp", dbname)
            table_name = dbname.split(".db")[0]
            fts_name = table_name + "_fts"

            decrypt(dbpath, temp_path, key)  # decrypt to temp file for modification

            conn = sqlite3.connect(temp_path)
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA journal_mode=OFF")
            trigger_exist = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' AND name='rebuild_trigger'"
            ).fetchone()
            if trigger_exist is None:
                print(f"{dbname} 中 {bcolors.BOLD}不存在{bcolors.ENDC} 触发器")
                proceed = input(
                    f"{bcolors.OKBLUE}要{bcolors.BOLD}注入{bcolors.ENDC}{bcolors.OKBLUE}触发器吗? (y/N): {bcolors.ENDC}"
                )
                if proceed.lower() != "y":
                    print(f"{bcolors.WARNING}跳过{dbname}{bcolors.ENDC}")
                    conn.close()
                    os.remove(temp_path)
                    continue
                conn.execute(
                    f"""
                    CREATE TRIGGER rebuild_trigger AFTER INSERT ON {table_name}
                    WHEN NEW."41701" = '__trigger_fts_rebuild__'
                    BEGIN
                        INSERT INTO {fts_name}({fts_name}) VALUES ('rebuild');
                    END;
                """
                )
            else:
                print(f"{dbname} 中 {bcolors.BOLD}已存在{bcolors.ENDC} 触发器")
                proceed = input(
                    f"{bcolors.OKBLUE}要{bcolors.BOLD}移除{bcolors.ENDC}{bcolors.OKBLUE}该触发器吗? (y/N): {bcolors.ENDC}"
                )
                if proceed.lower() != "y":
                    print(f"{bcolors.WARNING}保留现有触发器{bcolors.ENDC}")
                    conn.close()
                    os.remove(temp_path)
                    continue
                conn.execute("DROP TRIGGER IF EXISTS rebuild_trigger")

            conn.commit()
            conn.close()

            # encrypt modified database back to original location
            encrypt(dbpath, temp_path, key)

            os.remove(temp_path)  # remove temp file after encryption

import sqlite3
import os

from util import DB_INFO, bcolors, get_ntqq_base_path, encrypt, decrypt

if __name__ == "__main__":
    print(
        f"{bcolors.BOLD}{bcolors.WARNING}This script will inject triggers (or remove, if already injected) into your NTQQ databases{bcolors.ENDC}"
    )
    print(f"{bcolors.BOLD}{bcolors.WARNING}send:{bcolors.ENDC}")
    print(f"__trigger_fts_rebuild__")
    print(
        f"{bcolors.BOLD}{bcolors.WARNING}in the corresponding chat to trigger a rebuild (may lag NTQQ for a while){bcolors.ENDC}"
    )

    base_path = get_ntqq_base_path()
    key = input(f"{bcolors.OKBLUE}key: {bcolors.ENDC}")
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
                print(f"rebuild trigger {bcolors.BOLD}does not exist{bcolors.ENDC} in {dbname}")
                proceed = input(
                    f"{bcolors.OKBLUE}Do you want to {bcolors.BOLD}create{bcolors.ENDC}{bcolors.OKBLUE} it? (y/N): {bcolors.ENDC}"
                )
                if proceed.lower() != "y":
                    print(f"{bcolors.WARNING}Skipping trigger creation for {dbname}{bcolors.ENDC}")
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
                print(f"rebuild trigger {bcolors.BOLD}already exists{bcolors.ENDC} in {dbname}")
                proceed = input(
                    f"{bcolors.OKBLUE}Do you want to {bcolors.BOLD}remove{bcolors.ENDC}{bcolors.OKBLUE} it? (y/N): {bcolors.ENDC}"
                )
                if proceed.lower() != "y":
                    print(f"{bcolors.WARNING}Keeping existing trigger for {dbname}{bcolors.ENDC}")
                    conn.close()
                    os.remove(temp_path)
                    continue
                conn.execute("DROP TRIGGER IF EXISTS rebuild_trigger")

            conn.commit()
            conn.close()

            # encrypt modified database back to original location
            encrypt(dbpath, temp_path, key)

            os.remove(temp_path)  # remove temp file after encryption

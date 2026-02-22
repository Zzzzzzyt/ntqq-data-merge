import sqlite3
from util import DB_INFO, TIME_COLUMN, get_file_basename, get_rich_media_id, input_time, bcolors
import os


def build_dedup_insert(table, autoinc_column, columns, pk_column, dedupe_columns, start_time=None, end_time=None):
    # build equality predicate
    conditions = " AND ".join([f"m.{c} IS s.{c}" for c in dedupe_columns])
    if pk_column not in dedupe_columns:
        conditions = f"({conditions}) OR m.{pk_column} IS s.{pk_column}"
    columns_list = ", ".join([c for c in columns if c not in autoinc_column])

    select_sql = f"""
    SELECT {columns_list}
    FROM src_db.{table} s
    WHERE NOT EXISTS (
        SELECT 1 FROM main.{table} m
        WHERE {conditions}
    )
    """
    if TIME_COLUMN in columns:
        if start_time is not None:
            select_sql += f" AND s.{TIME_COLUMN} >= {start_time}"
        if end_time is not None:
            select_sql += f" AND s.{TIME_COLUMN} <= {end_time}"
    return columns_list, select_sql


def get_table_info(conn, table_name):
    table_info = conn.execute(f"PRAGMA src_db.table_info('{table_name}')").fetchall()
    autoinc_exists = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'").fetchall()
    columns = []
    autoinc_columns = []
    pk_column = None
    for cid, name, type, notnull, dflt_value, pk in table_info:
        name = '"{}"'.format(name)  # quote column name
        columns.append(name)
        if pk:
            pk_column = name
            if autoinc_exists:
                isautoinc = conn.execute("SELECT COUNT(*) FROM sqlite_sequence WHERE name=?", (table_name,)).fetchall()
                if isautoinc and isautoinc[0][0] > 0:
                    autoinc_columns.append(name)
    return columns, autoinc_columns, pk_column


def merge_uid_mapping(src_path, dst_path):
    print(f"{bcolors.INFO}Merging nt_uid_mapping_table...{bcolors.ENDC}")

    src_db = os.path.join(src_path, "nt_db/", "nt_msg.db")
    dst_db = os.path.join(dst_path, "nt_db/", "nt_msg.db")

    conn = sqlite3.connect(src_db)
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA journal_mode=OFF")
    src_mapping = conn.execute('SELECT "48901","48902","48912","1002" FROM nt_uid_mapping_table').fetchall()
    conn.close()

    src_mapping_dict = {row[1]: row for row in src_mapping}  # nt_uid -> (local_id, nt_uid, nt_uid2, old_uid)
    if len(src_mapping_dict) != len(src_mapping):
        print(f"{bcolors.WARNING}Warning: duplicate nt_uid(48902) found in source mapping???{bcolors.ENDC}")

    conn = sqlite3.connect(dst_db)
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA journal_mode=OFF")
    dst_mapping = conn.execute('SELECT "48901","48902","48912","1002" FROM nt_uid_mapping_table').fetchall()
    dst_mapping_dict = {row[1]: row for row in dst_mapping}  # nt_uid -> (local_id, nt_uid, nt_uid2, old_uid)
    if len(dst_mapping_dict) != len(dst_mapping):
        print(f"{bcolors.WARNING}Warning: duplicate nt_uid(48902) found in destination mapping???{bcolors.ENDC}")

    max_localid = conn.execute('SELECT MAX("48901") FROM nt_uid_mapping_table').fetchone()[0]
    current_localid = max_localid
    src2dst_localid_map = {}
    to_insert = []
    for src_localid, nt_uid, nt_uid2, old_uid in src_mapping:
        if nt_uid not in dst_mapping_dict:
            current_localid += 1
            src2dst_localid_map[src_localid] = current_localid
            to_insert.append((current_localid, nt_uid, nt_uid2, old_uid))
        else:
            dst_localid = dst_mapping_dict[nt_uid][0]
            src2dst_localid_map[src_localid] = dst_localid

    if to_insert:
        print(f"{bcolors.OKBLUE}Inserting {len(to_insert)} new uid mappings into destination...{bcolors.ENDC}")
        proceed = input(f"{bcolors.OKBLUE}Proceed to insert? (Y/n){bcolors.ENDC}")
        if proceed.lower() != "n":
            conn.executemany('INSERT INTO nt_uid_mapping_table ("48901","48902","48912","1002") VALUES (?,?,?,?)', to_insert)
            print(f"{bcolors.OKGREEN}Inserted {len(to_insert)} new uid mappings into destination{bcolors.ENDC}")
        else:
            print(f"{bcolors.WARNING}Skipping insertion of new uid mappings.{bcolors.ENDC}")
            raise Exception("User aborted uid mapping insertion.")

    conn.commit()
    conn.close()

    print(f"{bcolors.OKGREEN}Merged nt_uid_mapping_table, {len(src2dst_localid_map)} new local_id mapped{bcolors.ENDC}")

    return src2dst_localid_map


def merge_db(src_db, dst_db, dbinfo, src2dst_localid_map, start_time=None, end_time=None, close_session=True):
    print()
    print(f"{bcolors.INFO}Merging {src_db} into {dst_db}...{bcolors.ENDC}")

    conn = sqlite3.connect(dst_db)
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA journal_mode=OFF")
    triggers = None
    if src_db.endswith("fts.db"):
        # save triggers
        triggers = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'").fetchall()
        for trigger_name, _ in triggers:
            conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")

    try:
        conn.execute("ATTACH DATABASE ? AS src_db", (src_db,))
        for table_name, dedupe_columns in dbinfo.items():
            print(f"{bcolors.INFO}Merging table {table_name}...{bcolors.ENDC}")

            columns, autoinc_columns, pk_column = get_table_info(conn, table_name)

            # print(
            #     f"Table {table_name} columns: {columns}, autoinc: {autoinc_columns}, pk: {pk_column}, dedupe: {dedupe_columns}"
            # )

            columns_list, select_sql = build_dedup_insert(
                table_name, autoinc_columns, columns, pk_column, dedupe_columns, start_time=start_time, end_time=end_time
            )

            temp_table_core = f"temp_insert_{table_name}"
            temp_table_name = f'"{temp_table_core}"'
            index_name = f"idx_{temp_table_core}_dedupe"

            try:
                conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({', '.join(dedupe_columns)})")

                conn.execute(f"CREATE TEMP TABLE {temp_table_name} AS {select_sql}")

                if '"40027"' in columns:
                    # special handling for local_id in message tables
                    for src_localid, dst_localid in src2dst_localid_map.items():
                        conn.execute(f'UPDATE {temp_table_name} SET "40027" = ? WHERE "40027" = ?', (dst_localid, src_localid))

                rows_to_insert = conn.execute(f"SELECT COUNT(*) FROM {temp_table_name}").fetchone()[0]
                # print(conn.execute(f"SELECT * FROM {temp_table_name} LIMIT 5").fetchall())
                if rows_to_insert:
                    print(f"{bcolors.OKBLUE}Preparing to insert {rows_to_insert} rows into {table_name}{bcolors.ENDC}")
                    proceed = input(f"{bcolors.OKBLUE}Proceed to insert? (Y/n){bcolors.ENDC}")
                    if proceed.lower() == "n":
                        print(f"{bcolors.WARNING}Skipping insertion.{bcolors.ENDC}")
                        raise Exception("User aborted insertion.")
                    insert_sql = f"""
                    INSERT OR IGNORE INTO main.{table_name} ({columns_list})
                    SELECT {columns_list}
                    FROM {temp_table_name}
                    """
                    before_changes = conn.total_changes
                    conn.execute(insert_sql)
                    inserted_count = conn.total_changes - before_changes
                    print(f"{bcolors.OKGREEN}Inserted {inserted_count} rows into {table_name}{bcolors.ENDC}")
                else:
                    print(f"{bcolors.INFO}No new rows to insert for {table_name}{bcolors.ENDC}")
            finally:
                conn.execute(f"DROP INDEX IF EXISTS {index_name}")

                if close_session:
                    conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

        if triggers is not None:
            for trigger_name, trigger_sql in triggers:
                conn.execute(trigger_sql)
        conn.commit()
    except Exception as e:
        print(f"{bcolors.FAIL}Error merging {src_db} into {dst_db}: {e}{bcolors.ENDC}")
        conn.rollback()
    finally:
        conn.execute("DETACH DATABASE src_db")
    if close_session:
        conn.close()
    else:
        return conn


def merge_files_in_chat(src_path, dst_path, src2dst_localid_map, start_time=None, end_time=None):
    conn = merge_db(
        os.path.join(src_path, "nt_db/", "files_in_chat.db"),
        os.path.join(dst_path, "nt_db/", "files_in_chat.db"),
        DB_INFO["files_in_chat.db"],
        src2dst_localid_map,
        start_time=start_time,
        end_time=end_time,
        close_session=False,
    )
    assert conn is not None, "Connection should not be None when close_session is False"

    file_transfer_plan = set()
    for path, filename in conn.execute('SELECT "45403","45402" FROM temp_insert_files_in_chat_table').fetchall():
        if path is None and filename is None:
            continue
        basename1 = get_file_basename(os.path.basename(path)) if path else None
        basename2 = get_file_basename(filename) if filename else None
        if basename1 and basename2 and basename1 != basename2:
            print(f"{bcolors.WARNING}Filename mismatch: path {path} vs filename {filename}{bcolors.ENDC}")
        if basename1:
            file_transfer_plan.add(basename1)
        if basename2:
            file_transfer_plan.add(basename2)
    with open(os.path.join(src_path, "file_transfer_plan.txt"), "w") as f:
        for basename in sorted(file_transfer_plan):
            f.write(basename + "\n")

    conn.execute("DROP TABLE temp_insert_files_in_chat_table")
    conn.close()


def merge_rich_media(src_path, dst_path, src2dst_localid_map, start_time=None, end_time=None):
    conn = merge_db(
        os.path.join(src_path, "nt_db/", "rich_media.db"),
        os.path.join(dst_path, "nt_db/", "rich_media.db"),
        DB_INFO["rich_media.db"],
        src2dst_localid_map,
        start_time=start_time,
        end_time=end_time,
        close_session=False,
    )
    assert conn is not None, "Connection should not be None when close_session is False"

    rich_media_transfer_plan = set()
    for path, file_id in conn.execute('SELECT "45403","45503" FROM temp_insert_file_table').fetchall():
        if path is None or file_id is None:
            continue
        file_id = get_rich_media_id(file_id)
        rich_media_path = os.path.join(src_path, "rich_media/", file_id)
        if not os.path.exists(rich_media_path):
            print(f"{bcolors.WARNING}Rich media file {rich_media_path} does not exist, skipping.{bcolors.ENDC}")
            continue
        creation_time = os.path.getctime(rich_media_path)
        if start_time and creation_time < start_time:
            continue
        if end_time and creation_time > end_time:
            continue
        rich_media_transfer_plan.add((file_id, path))
    with open(os.path.join(src_path, "rich_media_transfer_plan.txt"), "w") as f:
        for file_id, path in sorted(rich_media_transfer_plan):
            f.write(f"{file_id}\t{path}\n")

    conn.execute("DROP TABLE temp_insert_file_table")
    conn.close()


if __name__ == "__main__":
    src_device = input(f"{bcolors.OKBLUE}Source device name: {bcolors.ENDC}")
    dst_device = input(f"{bcolors.OKBLUE}Destination device name: {bcolors.ENDC}")
    src_path = f"{src_device}_export/"
    dst_path = f"{dst_device}_export/"
    assert os.path.exists(src_path), f"Source path {src_path} does not exist"
    assert os.path.exists(dst_path), f"Destination path {dst_path} does not exist"

    print("You may specify a time range in merge source.")
    start_time = input_time(f"{bcolors.OKBLUE}Start time (YYYY-MM-DD, empty for no limit): {bcolors.ENDC}")
    end_time = input_time(f"{bcolors.OKBLUE}End time (YYYY-MM-DD, empty for no limit): {bcolors.ENDC}")

    src2dst_localid_map = merge_uid_mapping(src_path, dst_path)

    for dbname in DB_INFO.keys():
        if dbname in ["files_in_chat.db", "rich_media.db"]:
            continue  # handle separately due to file transfer plan
        merge_db(
            os.path.join(src_path, "nt_db/", dbname),
            os.path.join(dst_path, "nt_db/", dbname),
            DB_INFO[dbname],
            src2dst_localid_map,
            start_time=start_time,
            end_time=end_time,
        )

    merge_files_in_chat(src_path, dst_path, src2dst_localid_map, start_time=start_time, end_time=end_time)
    merge_rich_media(src_path, dst_path, src2dst_localid_map, start_time=start_time, end_time=end_time)

    print()
    print(f"{bcolors.INFO}Checking integrity of merged databases...{bcolors.ENDC}")

    for db_name in DB_INFO.keys():
        merged_db_path = os.path.join(dst_path, "nt_db/", db_name)
        # integrety check
        conn = sqlite3.connect(merged_db_path)
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA journal_mode=OFF")
        result = conn.execute("PRAGMA integrity_check").fetchone()
        if result[0] != "ok":
            print(f"{bcolors.FAIL}Integrity check failed for {merged_db_path}: {result[0]}{bcolors.ENDC}")
        else:
            print(f"{bcolors.OKGREEN}Integrity check passed for {merged_db_path}{bcolors.ENDC}")

        # vacuum to optimize the database after merging
        conn.execute("VACUUM")
        conn.commit()
        conn.close()

        filesize = os.path.getsize(merged_db_path)
        print(f"{bcolors.OKGREEN}Merged {db_name}, size: {filesize / (1024**2):.2f} MB{bcolors.ENDC}")

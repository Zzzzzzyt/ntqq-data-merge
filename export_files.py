import os
import sqlite3
import shutil
import pathlib
from tqdm import tqdm

from util import filesize_format, get_rich_media_id, bcolors


def copy_files(export_path, nt_data_path):
    print(f"{bcolors.INFO}正在计算文件体积...{bcolors.ENDC}")
    files = []
    for root, dirs, filenames in os.walk(nt_data_path):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            file_size = os.path.getsize(file_path)
            files.append((file_path, file_size))
    files.sort(key=lambda x: x[1], reverse=True)
    total_size = sum(file[1] for file in files)
    print(f"{bcolors.OKGREEN}找到 {len(files)} 个文件，总体积: {filesize_format(total_size)}{bcolors.ENDC}")
    proceed = input(f"{bcolors.OKBLUE}开始导出? (y/N){bcolors.ENDC}")
    if proceed.lower() == "y":
        with tqdm(total=total_size, unit="B", unit_scale=True) as pbar:
            for file_path, file_size in files:
                relative_path = os.path.relpath(file_path, nt_data_path)
                dest_path = os.path.join(export_path, "nt_data", relative_path)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                if not os.path.exists(dest_path):
                    shutil.copy2(file_path, dest_path)
                pbar.update(file_size)


def copy_rich_media(export_path):
    rich_media_db = os.path.join(export_path, "nt_db/rich_media.db")
    assert os.path.exists(rich_media_db), "未找到rich_media.db，请先运行export.py导出nt_db目录"
    conn = sqlite3.connect(rich_media_db)
    cursor = conn.cursor()

    # 45403 file_path
    # 45503 file_id

    cursor.execute('SELECT "45403","45503" FROM file_table')
    raw = cursor.fetchall()
    conn.close()

    rows = []
    total_size = 0
    for file_path, file_id in raw:
        if file_path and file_id:
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                rows.append((file_path, file_size, file_id))
                total_size += file_size

    rows.sort(key=lambda x: x[1], reverse=True)

    print(f"{bcolors.OKGREEN}找到 {len(rows)} 个下载的文件，总体积: {filesize_format(total_size)}{bcolors.ENDC}")
    proceed = input(f"{bcolors.OKBLUE}开始导出? (y/N){bcolors.ENDC}")
    if proceed.lower() == "y":
        os.makedirs(os.path.join(export_path, "rich_media/"), exist_ok=True)
        with tqdm(total=total_size, unit="B", unit_scale=True) as pbar:
            for file_path, file_size, file_id in rows:
                dest_path = os.path.join(export_path, "rich_media/", get_rich_media_id(file_id))
                if not os.path.exists(dest_path):
                    shutil.copy2(file_path, dest_path)
                pbar.update(file_size)


if __name__ == "__main__":
    from util import get_ntqq_base_path

    device_name = input(f"{bcolors.OKBLUE}设备名称: {bcolors.ENDC}")

    base_path = get_ntqq_base_path()

    export_path = f"{device_name}_export/"
    nt_data_path = os.path.join(base_path, "nt_qq/nt_data/")

    print(f"{bcolors.INFO}准备导出聊天图片/音频...{bcolors.ENDC}")
    copy_files(export_path, nt_data_path)

    print()
    print(f"{bcolors.INFO}准备导出下载的文件...{bcolors.ENDC}")
    copy_rich_media(export_path)

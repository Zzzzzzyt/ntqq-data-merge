import os
import shutil

from util import bcolors, filesize_format
import filetype

if __name__ == "__main__":
    from util import get_ntqq_base_path

    device_name = input(f"{bcolors.OKBLUE}设备名称: {bcolors.ENDC}")

    base_path = get_ntqq_base_path()

    export_path = f"{device_name}_export/"
    export_path = os.path.join(export_path, "fav_emoji")
    if not os.path.exists(export_path):
        os.makedirs(export_path)

    emoji_path = os.path.join(base_path, "nt_qq/nt_data/Emoji/personal_emoji/Ori/")
    if not os.path.exists(emoji_path):
        print(f"{bcolors.FAIL}未找到表情图片存储路径: {emoji_path}{bcolors.ENDC}")
        exit(1)

    total_size = 0
    for file in os.listdir(emoji_path):
        file_path = os.path.join(emoji_path, file)
        file_type = filetype.guess(file_path)
        ext = "jpg"
        if file_type:
            ext = file_type.extension
        output_file = os.path.join(export_path, os.path.splitext(file)[0] + "." + ext)
        shutil.copy2(file_path, output_file)
        total_size += os.path.getsize(file_path)
    print(
        f"{bcolors.OKGREEN}已导出 {len(os.listdir(export_path))} 张表情图片，总大小: {filesize_format(total_size)}{bcolors.ENDC}"
    )
    print(f"{bcolors.OKBLUE}导出路径: {os.path.abspath(export_path)}{bcolors.ENDC}")

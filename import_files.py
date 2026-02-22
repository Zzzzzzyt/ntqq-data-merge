from datetime import datetime
import os
import shutil
from tqdm import tqdm

from util import filesize_format, get_file_basename, bcolors


def transfer_files(file_list):
    todo = []
    conflicts = []
    for src_path, dst_path in file_list:
        src_size = os.path.getsize(src_path)
        if os.path.exists(dst_path):
            dst_size = os.path.getsize(dst_path)
            src_mod_time = os.path.getmtime(src_path)
            dst_mod_time = os.path.getmtime(dst_path)
            conflicts.append((src_path, dst_path, src_size, dst_size, src_mod_time, dst_mod_time))
        else:
            todo.append((src_path, dst_path, src_size))

    if len(conflicts) > 0:
        old_size = len(conflicts)
        replace = 0
        print(f"发现 {len(conflicts)} 个冲突:")
        strategy = input(f"{bcolors.OKBLUE}选择策略：(s)跳过, (o)覆盖, (n)取较新者, (b)取较大者: {bcolors.ENDC}").lower()
        if strategy == "s":
            conflicts = []
            replace = 0
        elif strategy == "o":
            for src_path, dst_path, src_size, dst_size, src_mod_time, dst_mod_time in conflicts:
                todo.append((src_path, dst_path, src_size))
            replace = old_size
        elif strategy == "n":
            for src_path, dst_path, src_size, dst_size, src_mod_time, dst_mod_time in conflicts:
                if src_mod_time > dst_mod_time:
                    todo.append((src_path, dst_path, src_size))
                    replace += 1
        elif strategy == "b":
            for src_path, dst_path, src_size, dst_size, src_mod_time, dst_mod_time in conflicts:
                if src_size > dst_size:
                    todo.append((src_path, dst_path, src_size))
                    replace += 1
        else:
            print("无效策略！")
            raise ValueError("Invalid strategy")
        print(f"{old_size - replace} 个文件被跳过，{replace} 个文件将被替换。")
    else:
        print("未发现冲突。")

    total_size = sum(src_size for _, _, src_size in todo)
    print(f"{len(todo)} 个文件待导入，总大小: {filesize_format(total_size)}")
    proceed = input(f"{bcolors.OKBLUE}是否继续导入文件？(y/N){bcolors.ENDC}")
    if proceed.lower() == "y":
        with tqdm(total=total_size, unit="B", unit_scale=True) as pbar:
            for src_path, dst_path, src_size in todo:
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                if not os.path.exists(dst_path):
                    shutil.copy2(src_path, dst_path)
                pbar.update(src_size)


if __name__ == "__main__":
    device_name = input(f"{bcolors.OKBLUE}设备名称: {bcolors.ENDC}")
    import_path = f"{device_name}_export/"

    from util import get_ntqq_base_path

    base_path = get_ntqq_base_path()
    data_path = os.path.join(import_path, "nt_data/")

    file_transfer_plan_path = os.path.join(import_path, "file_transfer_plan.txt")
    rich_media_transfer_plan_path = os.path.join(import_path, "rich_media_transfer_plan.txt")

    if os.path.exists(file_transfer_plan_path):
        file_transfer_plan = set()
        with open(file_transfer_plan_path, "r") as f:
            for line in f:
                s = line.strip()
                if s != "":
                    file_transfer_plan.add(s)
        print(f"{bcolors.OKGREEN}已加载聊天图片导入计划，包含 {len(file_transfer_plan)} 个文件。{bcolors.ENDC}")
        file_list = []
        for root, dirs, files in os.walk(data_path):
            for file in files:
                if get_file_basename(file) in file_transfer_plan:
                    src_path = os.path.join(root, file)
                    relative_path = os.path.relpath(src_path, data_path)
                    dst_path = os.path.join(base_path, "nt_qq/nt_data/", relative_path)
                    file_list.append((src_path, dst_path))
        transfer_files(file_list)
    else:
        print(f"{bcolors.WARNING}未在 {file_transfer_plan_path} 找到聊天图片导入计划，跳过。{bcolors.ENDC}")

    print()

    if os.path.exists(rich_media_transfer_plan_path):
        rich_media_transfer_plan = dict()
        with open(rich_media_transfer_plan_path, "r") as f:
            for line in f:
                s = line.strip()
                if s != "":
                    file_id, path = s.split("\t")
                    assert file_id not in rich_media_transfer_plan, f"重复的 file_id {file_id}"
                    rich_media_transfer_plan[file_id] = path

        rich_media_transfer_plan = sorted(rich_media_transfer_plan.items(), key=lambda x: x[1])

        if len(rich_media_transfer_plan) == 0:
            print(f"{bcolors.WARNING}下载文件导入计划为空。{bcolors.ENDC}")
            exit(0)

        print(f"{bcolors.OKGREEN}已加载下载文件导入计划，包含 {len(rich_media_transfer_plan)} 个文件。{bcolors.ENDC}")
        temp_path = os.path.join(import_path, "temp_rich_media/")
        print("稍后你可以选择在以下目录中创建备份:")
        print(f"{bcolors.BOLD}{os.path.abspath(temp_path)}{bcolors.ENDC}")

        for file_id, path in rich_media_transfer_plan:
            src_path = os.path.join(import_path, "rich_media/", file_id)
            if not os.path.exists(src_path):
                print(f"源文件 {src_path} 不存在，跳过。")
                continue
            print(f"{bcolors.BOLD}{path}{bcolors.ENDC}")
            print(f"体积: {filesize_format(os.path.getsize(src_path))}")
            print(f"创建时间: {datetime.fromtimestamp(os.path.getctime(src_path))}")
            print(f"修改时间: {datetime.fromtimestamp(os.path.getmtime(src_path))}")
            if os.path.exists(path):
                print(f"{bcolors.WARNING}目标文件已存在，将被覆盖！{bcolors.ENDC}")
            proceed = input(f"{bcolors.OKBLUE}复制该文件? (y)是/(N)否/(c)创建备份: {bcolors.ENDC}").lower()
            if proceed == "y":
                os.makedirs(os.path.dirname(path), exist_ok=True)
                shutil.copy2(src_path, path)
            elif proceed == "c":
                os.makedirs(temp_path, exist_ok=True)
                shutil.copy2(src_path, os.path.join(temp_path, os.path.basename(path)))
            print()
    else:
        print(f"{bcolors.WARNING}未在 {rich_media_transfer_plan_path} 找到下载文件导入计划，跳过下载文件导入。{bcolors.ENDC}")

from datetime import datetime
import os
import shutil
from tqdm import tqdm

from util import get_file_basename, bcolors


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
        print(f"Found {len(conflicts)} conflicts:")
        strategy = input(
            f"{bcolors.OKBLUE}Choose strategy: (s)kip, (o)verwrite, prefer (n)ew, prefer (b)ig: {bcolors.ENDC}"
        ).lower()
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
            print("Invalid strategy!")
            exit(1)
        print(f"{old_size - replace} files skipped, {replace} files to be replaced.")
    else:
        print("No conflicts found.")

    total_size = sum(src_size for _, _, src_size in todo)
    print(f"{len(todo)} files to be transferred, total size: {total_size / (1024**3):.2f} GiB")
    proceed = input(f"{bcolors.OKBLUE}Proceed to transfer files? (y/N){bcolors.ENDC}")
    if proceed.lower() == "y":
        with tqdm(total=total_size, unit="B", unit_scale=True) as pbar:
            for src_path, dst_path, src_size in todo:
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                if not os.path.exists(dst_path):
                    shutil.copy2(src_path, dst_path)
                pbar.update(src_size)


if __name__ == "__main__":
    device_name = input(f"{bcolors.OKBLUE}Device name: {bcolors.ENDC}")
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
        print(f"{bcolors.OKGREEN}Loaded file transfer plan with {len(file_transfer_plan)} files.{bcolors.ENDC}")
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
        print(
            f"{bcolors.WARNING}No file transfer plan found at {file_transfer_plan_path}, skipping file transfer.{bcolors.ENDC}"
        )

    if os.path.exists(rich_media_transfer_plan_path):
        rich_media_transfer_plan = dict()
        with open(rich_media_transfer_plan_path, "r") as f:
            for line in f:
                s = line.strip()
                if s != "":
                    file_id, path = s.split("\t")
                    assert file_id not in rich_media_transfer_plan, f"Duplicate file_id {file_id} in rich media transfer plan"
                    rich_media_transfer_plan[file_id] = path

        rich_media_transfer_plan = sorted(rich_media_transfer_plan.items(), key=lambda x: x[1])

        if len(rich_media_transfer_plan) == 0:
            print(f"{bcolors.WARNING}Rich media transfer plan is empty, skipping rich media transfer.{bcolors.ENDC}")
            exit(0)

        print(f"{bcolors.OKGREEN}Loaded rich media transfer plan with {len(rich_media_transfer_plan)} files.{bcolors.ENDC}")
        temp_path = os.path.join(import_path, "temp_rich_media/")
        print("You may copy rich media files to temp folder:")
        print(f"{bcolors.BOLD}{os.path.abspath(temp_path)}{bcolors.ENDC}")

        for file_id, path in rich_media_transfer_plan:
            src_path = os.path.join(import_path, "rich_media/", file_id)
            if not os.path.exists(src_path):
                print(f"Source file {src_path} does not exist, skipping.")
                continue
            print(f"{bcolors.BOLD}{path}{bcolors.ENDC}")
            print(f"size: {os.path.getsize(src_path) / (1024**2):.2f} MB")
            print(f"creation time: {datetime.fromtimestamp(os.path.getctime(src_path))}")
            print(f"modification time: {datetime.fromtimestamp(os.path.getmtime(src_path))}")
            if os.path.exists(path):
                print(f"{bcolors.WARNING}Note: destination file already exists, will be overwritten!{bcolors.ENDC}")
                continue
            proceed = input(
                f"{bcolors.OKBLUE}Proceed to transfer this rich media file? (y)es/(N)o/(c)opy to temp folder: {bcolors.ENDC}"
            ).lower()
            if proceed == "y":
                os.makedirs(os.path.dirname(path), exist_ok=True)
                shutil.copy2(src_path, path)
            elif proceed == "c":
                os.makedirs(temp_path, exist_ok=True)
                shutil.copy2(src_path, os.path.join(temp_path, os.path.basename(path)))
            print()
    else:
        print(
            f"{bcolors.WARNING}No rich media transfer plan found at {rich_media_transfer_plan_path}, skipping rich media transfer.{bcolors.ENDC}"
        )

import os


def get_documents_folder():
    # Source - https://stackoverflow.com/a/30924555
    # Posted by axil, modified by community. See post 'Timeline' for change history
    # Retrieved 2026-02-13, License - CC BY-SA 3.0

    import ctypes.wintypes

    CSIDL_PERSONAL = 5  # My Documents
    SHGFP_TYPE_CURRENT = 0  # Get current, not default value

    buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
    ctypes.windll.shell32.SHGetFolderPathW(None, CSIDL_PERSONAL, None, SHGFP_TYPE_CURRENT, buf)

    return buf.value


def get_ntqq_base_path():
    base = os.path.join(get_documents_folder(), "Tencent Files")
    arr=[]
    for number in os.listdir(base):
        if number.isdigit():
            arr.append(number)
    if len(arr) == 0:
        raise Exception("No account found in Tencent Files")
    elif len(arr) > 1:
        for i,number in enumerate(arr):
            print(f"{i}: {number}")
        index = int(input("Multiple qq accounts found. Please select the index of the folder to use: "))
        assert 0 <= index < len(arr), "Invalid index"
        return os.path.join(base, arr[index])
    else:
        print(f"Found account {arr[0]} in Tencent Files")
        return os.path.join(base, arr[0])
import argparse
import os
import platform
import sqlite3
from pathlib import Path
import time
from datetime import datetime
import identifiers


def create_database(db_filename):
    conn = sqlite3.connect(db_filename)
    c = conn.cursor()

    # Create directories table
    c.execute("""
        CREATE TABLE IF NOT EXISTS directories (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES directories(id),
            directory_name TEXT NOT NULL,
            full_path TEXT NOT NULL,
            permission TEXT
        )
    """)

    # Create files table with updated schema
    c.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY,
        directory_id INTEGER REFERENCES directories(id),
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        size INTEGER,
        permission TEXT
    )
""")

    # Create files table with updated schema
    c.execute("""
    CREATE TABLE IF NOT EXISTS tags (
        file_id INTEGER PRIMARY KEY REFERENCES files(id),
        identifiers TEXT,
        size TEXT,
        format TEXT,
        created DATETIME,
        accessed DATETIME,
        modified DATETIME,
        owner TEXT,
        indent INTEGER
    )
""")

    conn.commit()
    conn.close()


def insert_directory(conn, parent_id, directory_name, full_path, permission):
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO directories (parent_id, directory_name, full_path, permission)
        VALUES (?, ?, ?, ?)
    """, (parent_id, directory_name, full_path, permission))
    return c.lastrowid


def insert_file(conn, directory_id, file_name, file_path, size=None, permission=None):
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO files (directory_id, file_name, file_path, size, permission)
        VALUES (?, ?, ?, ?, ?)
    """, (directory_id, file_name, file_path, size, permission))
    return c.lastrowid


def insert_tag(conn, file_id, file_size=None, file_format=None, file_created=None, file_accessed=None, file_modified=None, file_owner=None, file_indent=None):
    c = conn.cursor()

    file_creation_datetime = datetime.fromtimestamp(file_created)
    file_accessed_datetime = datetime.fromtimestamp(file_accessed)
    file_modified_datetime = datetime.fromtimestamp(file_modified)
    file_identifiers = identifiers.IDENTIFIERS.get(file_format, None)

    c.execute("""
        INSERT OR IGNORE INTO tags (file_id, identifiers, size, format, created, accessed, modified, owner, indent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (file_id, file_identifiers, file_size, file_format, file_creation_datetime, file_accessed_datetime, file_modified_datetime, file_owner, file_indent))
    return c.lastrowid


def get_file_size_category(file_size_in_bytes):
    if file_size_in_bytes == 0:
        return "Empty"
    elif 0 < file_size_in_bytes <= 16 * 1024:
        return "Tiny"
    elif 16 * 1024 < file_size_in_bytes <= 1 * 1024 * 1024:
        return "Small"
    elif 1 * 1024 * 1024 < file_size_in_bytes <= 128 * 1024 * 1024:
        return "Medium"
    elif 128 * 1024 * 1024 < file_size_in_bytes <= 1 * 1024 * 1024 * 1024:
        return "Large"
    elif 1 * 1024 * 1024 * 1024 < file_size_in_bytes <= 4 * 1024 * 1024 * 1024:
        return "Huge"
    else:
        return "Gigantic"


def get_folder_depth(file_path):
    folder_depth = 0
    while file_path != os.path.dirname(file_path):
        folder_depth += 1
        file_path = os.path.dirname(file_path)
    return folder_depth


def get_directory_structure(conn, rootdir, filesystems=None, parent_id=None):
    try:
        if parent_id is None:
            # Insert root directory
            root_stat = os.stat(str(rootdir))
            root_id = insert_directory(
                conn, None, rootdir.name, str(rootdir), root_stat.st_mode)
            parent_id = root_id
        else:
            # Insert subdirectory
            sub_stat = os.stat(str(rootdir))
            sub_id = insert_directory(
                conn, parent_id, rootdir.name, str(rootdir), sub_stat.st_mode)
            parent_id = sub_id

        for item in os.listdir(rootdir):
            item_path = rootdir.joinpath(item)
            if os.path.islink(item_path):
                continue
            if os.path.isdir(item_path):
                if os.path.ismount(item_path) and not is_path_in_filesystems(item_path, filesystems):
                    # Skip directories on different filesystems
                    print(
                        f"Skipping '{item_path}' as it's not on the same filesystem")
                    continue
                try:
                    parent_id = get_directory_structure(
                        conn, item_path, filesystems, parent_id)
                except OSError as exc:
                    print(f"Failed to index '{item_path}': {exc}")
            else:
                file_stat = os.stat(str(item_path))
                file_id = insert_file(conn, parent_id, item, str(
                    item_path), file_stat.st_size, file_stat.st_mode)

                file_size = get_file_size_category(file_stat.st_size)
                file_format = os.path.splitext(str(item_path))[1][1:].lower()
                file_created = file_stat.st_ctime
                file_accessed = file_stat.st_atime
                file_modified = file_stat.st_mtime
                file_indent = get_folder_depth(str(item_path))

                # Get file owner (works on Linux and Windows)
                if os.name == 'posix':  # Linux
                    import pwd
                    file_owner = pwd.getpwuid(file_stat.st_uid).pw_name
                elif os.name == 'nt':  # Windows
                    import win32security
                    sd = win32security.GetFileSecurity(
                        str(item_path), win32security.OWNER_SECURITY_INFORMATION)
                    owner_sid = sd.GetSecurityDescriptorOwner()
                    file_owner = win32security.LookupAccountSid(None, owner_sid)[
                        0]

                insert_tag(conn, file_id, file_size, file_format,
                           file_created, file_accessed, file_modified, file_owner, file_indent)

    except (PermissionError) as exc:
        print(exc)
    return parent_id


def is_path_in_filesystems(path, filesystems):
    if platform.system() == "Windows":
        return True

    try:
        start_dir = str(path.resolve())
        for fs in filesystems:
            if start_dir.startswith(fs):
                return True
    except Exception as exc:
        print(exc)

    return False


def main(filesystems=None):
    db_filename = "file_index.db"

    if platform.system() == "Windows":
        root_directories = [Path("C:\\")]
        if filesystems is not None:
            filesystems = [fs for fs in filesystems if len(
                fs) <= 4 and fs[1] == ":"]
            root_directories.extend([Path(fs + "\\") for fs in filesystems])
    else:
        root_directories = [Path("/")]

    create_database(db_filename)
    conn = sqlite3.connect(db_filename)

    start_time = time.time()
    for root_directory in root_directories:
        get_directory_structure(conn, root_directory, filesystems)
    conn.commit()

    end_time = time.time()
    conn.close()
    print(
        f"Database file '{db_filename}' updated successfully in {end_time - start_time:.2f} seconds!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Create or update directory structure.')

    # Add example usage to the help output
    if platform.system() == "Windows":
        example_usage = """
        Example usage on Windows:
        python <file> -f D:\\ E:\\
        """
    else:
        example_usage = """
        Example usage on Linux / Mac:
        python <file> -f /mnt/c /mnt/e
        """

    parser.description += example_usage

    parser.add_argument('-f', '--filesystems', type=str,
                        nargs='+', help='Other file systems to scan')

    args = parser.parse_args()

    main(args.filesystems)

#!/usr/bin/env python3
#>>>
# 
# Copyright (c) 2025 EQUA Simulation AB
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
#<<<

from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED, ZIP_STORED
import os
from datetime import datetime
from dataclasses import dataclass, asdict
import json
import time
import shutil
import filelock
from werkzeug.utils import secure_filename
from flask import jsonify


# There is a lot of root and path here. I guess I'm trying to keep the
# file operations sandboxed somehow, only using absolute paths internally,
# whereas the web interface only uses relative paths (to CFD_HOME e.g.).

tfmt = '%Y-%m-%d %H:%M:%S'


@dataclass
class file_info:
    name: str
    path: Path
    file_size: int
    mtime: int
    last_modified: datetime
    file_type: str

    def asdict(self):
        return asdict(self)

    def asjson(self):
        return json.dumps(self.asdict(), indent=4, default=str)

    def mtime_str(self):
        return self.last_modified.strftime(tfmt)

    def __repr__(self):
        return self.asjson()


def get_dir_size(path: Path, scale: int = 1024**2):
    total = 0
    for entry in path.iterdir():
        if entry.is_file():
            total += entry.stat().st_size/scale
        elif entry.is_dir():
            total += get_dir_size(entry)
    return total


def list_directory(root: Path, path: Path, forced_type=None) -> dict:
    content = {'dirs': [], 'files': []}
    top_level = Path(root, path)
    if not top_level.is_dir():
        top_level = Path(root)
    try:
        for item in top_level.iterdir():
            f_info = f_stat(root, item.relative_to(root), forced_type)
            if f_info:
                if item.is_dir():
                    content['dirs'].append(f_info)
                else:
                    content['files'].append(f_info)
    except Exception as e:
        pass
    return content


def list_directory_as_dicts(root: Path, path: Path) -> list:
    content = list_directory(root, path)
    dictified = [v.asdict() for v in content['dirs']]
    dictified+= [v.asdict() for v in content['files']]
    asjson = json.dumps(dictified, default=str)
    dictified = json.loads(asjson)
    return dictified


def f_stat(root: Path, path: Path, forced_type=None) -> dict:
    abs_path = Path(root, path)
    mtime = abs_path.stat().st_mtime
    if abs_path.is_dir():
        size = get_dir_size(abs_path)
        file_type = 'directory'
    elif abs_path.is_file():
        size = abs_path.stat().st_size
        file_type = forced_type or path.suffix
    else:
        return None
    f_info = file_info(path=path,
                       name=abs_path.name,
                       mtime=mtime,
                       last_modified=datetime.fromtimestamp(mtime),
                       file_size=int(size),
                       file_type=file_type)
    return f_info


def find_openfoam_cases(root: Path):
    root = Path(root)
    if not os.access(root, os.R_OK):
        return f'{root} is not readable.'
    for item in root.rglob('*'):
        if item.is_file() and item.name == 'controlDict':
            case_path = item.parent.parent
            try:
                relative_root = case_path.relative_to(root)
            except ValueError:
                continue
            yield f_stat(root, relative_root)


def zip_directory(case_path: Path, target_file: Path):
    case_root = case_path.parent
    files = [f for f in case_path.rglob('*') if f.is_file()]
    if files and generate_zip(files, target_file, root=case_root, compress=False):
        return target_file
    return None


def generate_zip(files: list, zip_file: str, root=Path(os.sep), compress=False):
    comp = ZIP_DEFLATED if compress else ZIP_STORED
    try:
        with ZipFile(zip_file, mode="w", compression=comp) as zf:
            for f in files:
                if Path(f).exists():
                    tgt_path = Path(f).relative_to(root)
                    zf.write(f, arcname=tgt_path.as_posix())
    except Exception:
        return False
    return True


def safe_getmtime(fpath):
    """Stale symbolic links breaks getmtime"""
    if not os.path.islink(fpath):
        return os.path.getmtime(fpath)


def dir_age(path, now):
    """Recurse loop all files under path and find the age of youngest file."""
    youngest_file_mtime = 0

    for root, dirs, files in os.walk(path, topdown=False):
        t_files = [os.path.join(root, f) for f in files if safe_getmtime(os.path.join(root, f))]
        if t_files:
            youngest_file_mtime = max([max([os.path.getmtime(f) for f in t_files]), youngest_file_mtime])
    return now - youngest_file_mtime


def remove_old_folders(path: Path, hours=48):
    now = datetime.now().timestamp()
    max_age = hours * 3600
    case_dirs = [path/c.path for c in find_openfoam_cases(path)]
    for dir in case_dirs:
        if dir_age(dir, now) > max_age:
            try:
                shutil.rmtree(dir)
            except Exception as e:
                print(f'Could not remove directory. {e}', flush=True)
    # Cleanup empty top level folders
    for d in (d for d in path.iterdir() if d.is_dir()):
        if not any(d.iterdir()):
            os.rmdir(d)


def remove_old_files(path: Path, hours=48):
    """Again using safe_getmtime. This function will not touch symbolic
    links!"""
    now = datetime.now().timestamp()
    max_age = hours * 3600
    for f in (f for f in path.iterdir() if f.is_file()):
        mtime = safe_getmtime(f)
        if mtime and (now - mtime) > max_age:
            f.unlink()



def post_request_file_wait(uploaded_file, target_folder: Path):
    """Post file using its stream."""
    
    if not uploaded_file or not uploaded_file.filename:
        return False

    file_name = secure_filename(uploaded_file.filename)
    target_file = Path(target_folder, file_name)

    temp_file = target_file.with_suffix('.part')
    with temp_file.open('wb') as f:
        # Read directly from the stream of the FileStorage object
        chunk_size = 4096
        while True:
            chunk = uploaded_file.stream.read(chunk_size)
            if len(chunk) == 0:
                break
            f.write(chunk)
    lock_file = target_file.with_suffix('.lock')
    with filelock.FileLock(lock_file):
        old_file = target_file.with_suffix('.old')
        if target_file.exists():
            target_file.rename(old_file)
        temp_file.rename(target_file)
        if target_file.exists() and old_file.exists():
            old_file.unlink()
        if lock_file.exists():
            lock_file.unlink()

    return True

# def post_request_file_wait(request, target_file: Path):
#     if request.content_length > 0:
#         temp_file = target_file.with_suffix('.part')
#         with temp_file.open('wb') as f:
#             chunk_size = 4096
#             while True:
#                 chunk = request.stream.read(chunk_size)
#                 if len(chunk) == 0:
#                     break
#                 f.write(chunk)
#         with filelock.FileLock(target_file.with_suffix('.lock')):
#             old_file = target_file.with_suffix('.old')
#             if target_file.exists():
#                 target_file.rename(old_file)
#             temp_file.rename(target_file)
#             if target_file.exists() and old_file.exists():
#                 old_file.unlink()


def get_request_file_wait(target_file: Path):
    try:
        with filelock.FileLock(target_file.with_suffix('.lock'), timeout=10):
            return True
    except filelock.Timeout:
        return False


if __name__ == '__main__':
    import sys
    root = sys.argv[1]
    cases = find_openfoam_cases(root)
    for cas in cases:
        print(cas)
        print(cas.path)

    contents = list_directory_as_dicts(root,'')
    print(json.dumps(contents, default=str, indent=2))

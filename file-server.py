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

from file_utils import find_openfoam_cases, zip_directory
from file_utils import list_directory, list_directory_as_dicts
from file_utils import remove_old_folders, remove_old_files
from file_utils import post_request_file_wait, get_request_file_wait

from werkzeug.middleware.proxy_fix import ProxyFix
from flask import Flask, render_template, request,\
    send_from_directory, abort, jsonify, redirect, url_for
from werkzeug.utils import secure_filename
from pathlib import Path
import json
import os
import sys
import getpass
from random import randint

if not os.getenv('CFD_HOME'):
    print('Error: CFD_HOME variable must be set to the files target directpory')
    sys.exit(1)

app = Flask(__name__)
uploads = os.getenv('CFD_UPLOAD_FOLDER') or (os.getenv('CFD_HOME') + '/uploads')
print(uploads)

if not (uploads and Path(uploads).is_dir()):
    print('Error: CFD_UPLOAD_FOLDER or CFD_HOME not set to a directory', flush=True)
    sys.exit(1)

app.config['CFD_UPLOAD_FOLDER'] = Path(uploads)
app.config['CFD_UPLOAD_FOLDER'].mkdir(parents=True, exist_ok=True)
app.wsgi_app = ProxyFix(app.wsgi_app, x_host=1, x_prefix=1)


@app.route('/')
def test():
    return {"test": True}


@app.get('/upload')
def upload():
    arg = request.args.get('message')
    message = 'Upload file from local drive'
    if arg is not None:
        try:
            message = json.loads(arg).get('message') or message
        except:
            pass
    return render_template('upload.html', message=message)


def safe_path(root: Path, path: Path):
    try:  # Protect against access outside of root
        (root/path).resolve().relative_to(root)
        return True
    except Exception:
        return False


@app.get('/explore/')
@app.get('/explore/<path:case_path>')
def explorer(case_path=None):
    root_path = Path(os.getenv('CFD_HOME'))
    case_path = Path(case_path) if case_path else Path('.')
    if not safe_path(root_path, case_path):
        abort(404)
    if Path(root_path, case_path).is_file():
        try:
            content = Path(root_path, case_path).open('rb').read().decode()
        except ValueError:
            content = 'Binary data file'
        return render_template('file.html',
                               header=case_path.name,
                               file_name=case_path.name,
                               message=content)
    content = list_directory(root_path, case_path)
    return render_template('explore.html',
                           header=case_path,
                           parent=case_path.parent,
                           dir_info_list=content.get('dirs'),
                           file_info_list=content.get('files'))

@app.route('/uploads/')
def uploads():
    root_path = app.config['CFD_UPLOAD_FOLDER']
    content = list_directory(root_path, '.')
    files = content.get('files')
    files.sort(key=lambda t: t.mtime, reverse=True)
    return render_template('explore_uploads.html',
                           header='Transferred files (Zip format)',
                           parent=None,
                           dir_info_list=[],
                           file_info_list=files)


def download_base(root_path, case_path):
    """Download function (and routes) used for manual downloading from 
    explore pages"""
    file_path = Path(root_path, case_path)
    if file_path.is_file():
        return send_from_directory(root_path,
                                   case_path,
                                   as_attachment=True,
                                   download_name=str(case_path))
    elif file_path.is_dir():
        dl_name = request.args.get('dl') or 'download.zip'
        target_file = root_path/dl_name
        if zip_directory(file_path, target_file):
            ret = send_from_directory(root_path,
                                      dl_name,
                                      as_attachment=True)
            try:
                target_file.unlink()
            except:
                pass
            return ret


@app.route('/download/<path:case_path>')
def download(case_path):
    root_path = Path(os.getenv('CFD_HOME'))
    case_path = Path(case_path)
    ret = download_base(root_path, case_path)
    return ret or abort(404)


@app.post('/rw')
@app.get('/rw/<path:file_name>')
def file_transfer(file_name=None):
    """Post or get file using Flask methods"""
    target_folder = app.config['CFD_UPLOAD_FOLDER']

    if request.method == 'POST':
        file_object = request.files.get('file')
        if Path(target_folder, file_object.filename).exists():
            msg = json.dumps({'message':
                              'File exists on server. Please rename before uploading'})
            return redirect(url_for('upload', message=msg))
        if post_request_file_wait(request.files.get('file'), target_folder):
            return redirect(url_for('explorer'))
        else:
            return jsonify({'error': 'No file selected for upload'}), 400
 
    elif request.method == 'GET':
        target_file = Path(target_folder, secure_filename(file_name))
        if target_file.exists():
            if get_request_file_wait(target_file):
                return send_from_directory(target_folder, file_name)
            else:
                return json.dumps({'exit': 404,
                                   'message': 'File timeout on server.'})
        else:
            return json.dumps({'exit': 404,
                               'message': 'File not found on server.'})
    return request.headers.__repr__()


def get_uploads(file_name=None):
    target_folder = app.config['CFD_UPLOAD_FOLDER']
    if target_folder.is_dir():
        content = list_directory_as_dicts(target_folder, Path('.'))
        if file_name:
            for item in content:
                if isinstance(item, dict) and item.get('path') == file_name:
                    return item
            return {}
        content.sort(key=lambda t: t.get('mtime'), reverse=True)
        return content
    return []


@app.get('/api/ls', defaults={'file_name': None})
@app.get('/api/ls/<path:file_name>')
def ls_uploads(file_name):
    content = get_uploads(file_name)
    return jsonify(content)


@app.route('/cleanup_folders')
def cleanup_folders(redirect=True):
    remove_old_folders(Path(os.getenv('CFD_HOME')), hours=2*24)
    remove_old_files(Path(app.config['CFD_UPLOAD_FOLDER']), hours=2*24)
    if redirect:
        return redirect(url_for('explorer'))


def log():
    log_dir = Path(f'/tmp/file-server-{os.getpid()}')
    print(log_dir)
    if not log_dir.exists():
        log_dir.mkdir()
    with Path(log_dir, 'PID').open('w') as f:
        f.write(str(os.getpid()))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except TypeError as e:
            print('Argument must be a port number')
            print(e)
            sys.exit(1)
    else:
        port = 5000
    log()
    app.run(debug=True, host='0.0.0.0', port=port)

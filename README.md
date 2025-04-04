# libression
## About this project
- Self-hosting media organiser
- Meaning behind the name Libression (term coined by [@yaxxie](https://github.com/yaxxie)):
  - Librarian (organiser)
  - Libre (free, open source, self-hosting)
  - Impression (media/photos/videos)

## Overview
This codebase contains 2 main components:
- [libression API](./libression/) (RPC-style API)
  - depends on an accessible server that can process [webdav protocol](https://en.wikipedia.org/wiki/WebDAV) to interact with files/folders
- [frontend webapp](./frontend/)

## Setup
- Set up storage server (e.g. nginx+webdav)
  - For a sample setup see [here](./sample_deployment/README.md)
- Deploy the libression API (with appropriate permissions to access the storage server)
  - e.g. for linux Fedora:
    - Install encodings (`sudo dnf install ffmpeg ffmpeg-devel libheif libffi libheif-devel libde265-devel`)
    - Set up python env with poetry (cd into directory and do `poetry install`)
    - Run app with command `poetry run python main.py`
- Deploy the frontend web app (with appropriate permissions to access both libression API and storage servers)
  - ```
    # starting from repo root (assuming npm is installed)
    cd frontend
    npm install
    npm run dev
    ```

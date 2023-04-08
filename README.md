# libression
## About this project
- Self-hosting media organiser
- Meaning behind the name Libression:
  - Librarian (organiser)
  - Libre (free, open source, self-hosting)
  - Impression (media/photos/videos)
- (term coined by [@yaxxie](https://github.com/yaxxie))

## Quickstart

### Minio
- Minio can serve directories/files (`</path/of/where/you/store/media>`) as an S3 object store
- That said, `minio.RELEASE.2022-05-26T05-48-41Z` is the last version that can directly serve 
- directories/files as an S3 object store (and vice versa)
- We need this for our web app to interact with this content
- Vaguely follow this ([guide](https://min.io/download#_)), but install the archived version!
  - on Linux (amd64):
    - ```
      wget -O minio https://dl.min.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2022-05-26T05-48-41Z

      chmod +x minio

      MINIO_ROOT_USER=minioadmin \
      MINIO_ROOT_PASSWORD=miniopassword \
      ./minio server </path/of/where/you/store/media> \
        --console-address ":9001"
      ```
  - on Mac / Windows
    - Not tested yet. Should be similar to above!

### Web app
- [Install prerequisites for Wand](https://docs.wand-py.org/en/latest/#requirements)
- Install ffmpeg and other encodings, e.g.
    - `sudo dnf install ffmpeg ffmpeg-devel libheif`  # libheif not really used yet...
- Set up python env with poetry
- Install dependencies (cd into directory and do `poetry install`)
- Run app with command `poetry run main.py`
- Open Web UI at `http://localhost:8000/`

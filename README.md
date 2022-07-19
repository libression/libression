# Libression
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
- We need this for our web app to interact with this content
- Install and run ([guide](https://min.io/download#_))
  - on Linux (amd64):
    - ```
      wget https://dl.min.io/server/minio/release/linux-amd64/minio

      chmod +x minio

      MINIO_ROOT_USER=minioadmin \
      MINIO_ROOT_PASSWORD=miniopassword \
      ./minio server </path/of/where/you/store/media> \
        --console-address ":9001"
      ```
  - on Mac
    - ```
      brew install minio/stable/minio
      
      MINIO_ROOT_USER=minioadmin \
      MINIO_ROOT_PASSWORD=miniopassword \
      minio server </path/of/where/you/store/media> \
        --console-address ":9001"
      ```
  - on Windows (sort yourself out)
  
### Web app
- [Install prerequisites for Wand](https://docs.wand-py.org/en/0.6.8/#requirements)
- Install pip requirements (`pip install -r requirements.txt`)
- Run with command `python main.py`
- Open Web UI at `http://localhost:8000/`

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

### Nginx + Webdav
- Fedora
  - Install nginx
    ```
    sudo dnf install nginx httpd-tools
    ```
    - Configure nginx (/etc/nginx/conf.d/webdav.conf, needs sudo)
    ```
    events {
        worker_connections 1024;  # Adjust as needed
    }

    http {
        include       mime.types;
        default_type application/octet-stream;

        sendfile        on;
        keepalive_timeout  65;

        server {
            listen 80;
            server_name localhost;  # Use 'localhost' for local access

            auth_basic "Restricted Access";
            auth_basic_user_file /usr/local/etc/nginx/.htpasswd;

            # First libression_library directory
            location /dummy_photos/ {
                root /Users/ernest/Downloads;

                dav_methods PUT DELETE MKCOL COPY MOVE;
                # dav_ext_methods PROPFIND OPTIONS;

                dav_access user:rw group:rw all:r;

                client_max_body_size 0;
                create_full_put_path on;

                autoindex on;
            }

            # Second libression_cache directory
            location /dummy_photos_cache/ {
                root /Users/ernest/Downloads;

                dav_methods PUT DELETE MKCOL COPY MOVE;
                # dav_ext_methods PROPFIND OPTIONS;

                dav_access user:rw group:rw all:r;

                client_max_body_size 0;
                create_full_put_path on;

                autoindex on;
            }
        }
    }

    ```
  - **Create a password file for basic authentication**: bash
    ```
    sudo dnf install httpd-tools
    sudo htpasswd -c /etc/nginx/.htpasswd your_username
    ```
  - **Set permissions and SELinux context**: bash
    ```
    sudo chown -R nginx:nginx /path/to/your/folder
    sudo chmod -R 755 /path/to/your/folder
    sudo semanage fcontext -a -t httpd_sys_content_t "/path/to/your/folder(/.)?"
    sudo restorecon -R -v /path/to/your/folder
    sudo setsebool -P httpd_can_network_connect 1
    sudo setsebool -P httpd_can_write_content 1
    ```
  - **Start and enable Nginx**:
    ```
    sudo systemctl enable nginx
    sudo systemctl start nginx
    sudo firewall-cmd --permanent --add-service=http
    sudo firewall-cmd --reload
    ```
- macos:
  - brew install nginx --with-dav
  - brew install httpd, config /opt/homebrew/etc/nginx/nginx.conf (above file)
  - set htpasswd: `sudo htpasswd -c /usr/local/etc/nginx/.htpasswd your_username`  (chilledgeek/chilledgeek)
  - restart nginx: `brew services restart nginx`
  - check status `nginx -t`
  

# Lib Magic
- `sudo apt-get install libmagic1`

### Web app
- Install a bunch of encodings, e.g.
    - `sudo dnf install ffmpeg ffmpeg-devel libheif libffi libheif-devel libde265-devel`
- Set up python env with poetry
- Install dependencies (cd into directory and do `poetry install`)
- Run app with command `poetry run python main.py`
- Open Web UI at `http://localhost:8000/`

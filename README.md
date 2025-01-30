# libression
## About this project
- Self-hosting media organiser
- Meaning behind the name Libression:
  - Librarian (organiser)
  - Libre (free, open source, self-hosting)
  - Impression (media/photos/videos)
- (term coined by [@yaxxie](https://github.com/yaxxie))

## Quickstart
We need 3 things:
- Storage (e.g. Minio or Webdav)
- Functions (e.g. Libression web API)
- Web UI (e.g. NextJS)

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
#### Linux
- Parameters to set up (replace with appropriate values)
  - `/var/www/webdav/` (root directory for webdav, not sure if this can be changed...)
  - `libression_photos` (directory to expose, should be in `/var/www/webdav/`)
  - `libression_photos_cache` (cache directory, should be in `/var/www/webdav/`, can be empty to start with)
  - webdav username: chilledgeek
  - webdav password: chilledgeek
  - webdavsecret key: chilledgeek_secret_key (for presigned URLs)

- Notes (from setup on Fedora)
  - Install nginx with `sudo dnf install nginx nginx-all-modules httpd-tools`
  - Install secure link nginx `sudo dnf install 'nginx-mod*'`
  - Configure nginx
    - Modify the `/etc/nginx/conf.d/webdav.conf` file
      - Only the `server` block is required (refer to [`samples/linux_webdav.conf`](samples/linux_webdav.conf))
      - The rest should be in `/etc/nginx/nginx.conf` (comment out the server block if it clashes)
    - run `sudo nginx -t` to test the configs are working
  - Create folders + permissions
    ``` bash
    sudo groupadd webdav
    sudo usermod -a -G webdav $USER
    sudo usermod -a -G webdav nginx
    sudo mkdir -p /var/www/webdav

    sudo dnf install openssl  # for self-signed cert generation
    sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout /etc/nginx/cert.key \
        -out /etc/nginx/cert.crt

    sudo htpasswd -c /etc/nginx/.htpasswd chilledgeek  # prompts for password for basic authentication
    sudo chown -R nginx:nginx /var/www/webdav  # set permissions and SELinux context
    sudo chmod -R 755 /var/www/webdav
    sudo semanage fcontext -a -t httpd_sys_content_t "/var/www/webdav(/.)?"
    sudo restorecon -R -v /var/www/webdav

    # Set context
    sudo semanage fcontext -a -t httpd_sys_content_t "/var/www/webdav/libression_photos(/.*)?"
    sudo semanage fcontext -a -t httpd_sys_content_t "/var/www/webdav/libression_photos_cache(/.*)?"
    sudo restorecon -R -v /var/www/webdav/libression_photos
    sudo restorecon -R -v /var/www/webdav/libression_photos_cache

    # Set additional required SELinux booleans
    sudo setsebool -P httpd_unified 1
    sudo setsebool -P httpd_can_network_connect 1
    sudo setsebool -P httpd_can_network_relay 1
    sudo setsebool -P httpd_anon_write 1
    sudo setsebool -P httpd_read_user_content 1
    sudo setsebool -P httpd_enable_homedirs 1
    sudo setsebool -P daemons_enable_cluster_mode 1

    ```
  - Setup and run nginx
    ```
    sudo systemctl enable nginx
    sudo systemctl start nginx
    sudo firewall-cmd --permanent --add-service=http
    sudo firewall-cmd --reload
    ```

#### MacOS:
  - ``` bash
    # Set up https certs
    mkdir -p ~/certs
    cd ~/certs
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout nginx-selfsigned.key -out nginx-selfsigned.crt

    # Install nginx
    brew install nginx --with-dav
    brew install httpd
    ```
  - Modify `/opt/homebrew/etc/nginx/nginx.conf` correspondingly (refer to [`samples/mac_nginx.conf`](samples/mac_nginx.conf))
  - Setup and run nginx
    ``` bash
    nginx -t  # test the configs are working
    sudo htpasswd -c /usr/local/etc/nginx/.htpasswd chilledgeek  # prompts for password (chilledgeek?)

    brew services restart nginx
    nginx -t
    ```

# Lib Magic
- `sudo apt-get install libmagic1`

### Libression web API
- Install encodings, e.g. for linux Fedora:
  - `sudo dnf install ffmpeg ffmpeg-devel libheif libffi libheif-devel libde265-devel`
- Set up python env with poetry
- Install dependencies (cd into directory and do `poetry install`)
- Run app with command `poetry run python main.py`
- Open Web UI at `http://localhost:8000/`


# NEW NOTES TO BE EDITTED


Script ( in CI/local testing for linux docker setup)
```
#!/bin/bash
# Generate self-signed certificates
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout cert.key \
    -out cert.crt \
    -subj "/C=US/ST=Test/L=Test/O=Test/CN=localhost"

# Create htpasswd file
htpasswd -cb .htpasswd libression_user libression_password
```

docker compose -f docker-compose.yml up -d




# Frontend
cd frontend
npm install
npm run dev

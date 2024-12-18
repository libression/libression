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
- credentials setup:
  - username: chilledgeek
  - password: chilledgeek
  - secret key: chilledgeek_secret_key

- Fedora
  - Create folder to expose
    ```
    # Create a new group
    sudo groupadd webdav

    # Add your user and nginx to the group
    sudo usermod -a -G webdav $USER
    sudo usermod -a -G webdav nginx

    # Create a new directory for the WebDAV content
    sudo mkdir -p /var/www/webdav

    # Set ownership and permissions
    sudo chown -R $USER:webdav /var/www/webdav
    sudo chmod -R 770 /var/www/webdav 

    # Generate self-signed cert for Nginx
    sudo dnf install openssl
    sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout /etc/nginx/cert.key \
        -out /etc/nginx/cert.crt

    ```
  - Install nginx
    ```
    sudo dnf install nginx nginx-all-modules httpd-tools
    ```
    - Configure nginx (/etc/nginx/conf.d/webdav.conf, needs sudo ... only needs the server block ... the rest should be in `/etc/nginx/nginx.conf` ... comment out the server bits in there)
    ```
    server {
        listen 443 ssl;
        server_name localhost;  # Use 'localhost' for local access

        ssl_certificate /etc/nginx/cert.crt;
        ssl_certificate_key /etc/nginx/cert.key;

        auth_basic "Restricted Access";
        auth_basic_user_file /etc/nginx/.htpasswd;

        # First libression_library directory
        location /photos_to_print_copy {
            return 301 $scheme://$host$uri/;
        }

        location /photos_to_print_copy/ {
            alias /var/www/webdav/photos_to_print_copy/;

            dav_methods PUT DELETE MKCOL COPY MOVE;
            dav_access user:rw group:rw all:r;

            # Allow all WebDAV methods
            add_header Allow "GET,HEAD,PUT,DELETE,MKCOL,COPY,MOVE,PROPFIND,OPTIONS" always;
            add_header DAV "1,2" always;
            add_header MS-Author-Via "DAV" always;

            client_max_body_size 0;
            create_full_put_path on;
            autoindex on;

            # Allow all methods including PROPFIND
            if ($request_method !~ ^(GET|HEAD|PUT|DELETE|MKCOL|COPY|MOVE|PROPFIND|OPTIONS)$) {
                return 405;
            }
        }

        # Secure downloads
        location /secure/photos_to_print_copy/ {
            alias /var/www/webdav/photos_to_print_copy/;

            # Secure link configuration
            secure_link $arg_md5,$arg_expires;
            secure_link_md5 "$secure_link_expires$uri chilledgeek_secret_key";

            if ($secure_link = "") {
                return 403;
            }
            if ($secure_link = "0") {
                return 410;
            }

            # Only allow GET
            limit_except GET {
                deny all;
            }
        }
                         
        # Second libression_cache directory
        location /libression_cache {
            return 301 $scheme://$host$uri/;
        }
        location /libression_cache/ {
            alias /var/www/webdav/libression_cache/;

            dav_methods PUT DELETE MKCOL COPY MOVE;
            dav_access user:rw group:rw all:r;

            # Allow all WebDAV methods
            add_header Allow "GET,HEAD,PUT,DELETE,MKCOL,COPY,MOVE,PROPFIND,OPTIONS" always;
            add_header DAV "1,2" always;
            add_header MS-Author-Via "DAV" always;

            client_max_body_size 0;
            create_full_put_path on;
            autoindex on;

            # Allow all methods including PROPFIND
            if ($request_method !~ ^(GET|HEAD|PUT|DELETE|MKCOL|COPY|MOVE|PROPFIND|OPTIONS)$) {
                return 405;
            }
        }

        location /secure/libression_cache/ {
            alias /var/www/webdav/libression_cache/;

            secure_link $arg_md5,$arg_expires;
            secure_link_md5 "$secure_link_expires$uri chilledgeek_secret_key";

            if ($secure_link = "") {
                return 403;
            }
            if ($secure_link = "0") {
                return 410;
            }

            limit_except GET {
                deny all;
            }
        }
    }

    ```
  - **Create a password file for basic authentication**: bash
    ```
    sudo htpasswd -c /etc/nginx/.htpasswd <your_username>
    ```
  - **Set permissions and SELinux context**: bash
    ```
    sudo chown -R nginx:nginx /path/to/your/folder
    sudo chmod -R 755 /path/to/your/folder
    # Make parent directory accessible
    sudo chmod 755 /path/to/your


    # sudo semanage fcontext -a -t httpd_sys_content_t "/path/to/your/folder(/.)?"  # TODO: check if needed
    # sudo restorecon -R -v /path/to/your/folder



    # Set context
    sudo semanage fcontext -a -t httpd_sys_content_t "/home/edesktop/Downloads/photos_to_print_copy(/.*)?"
    sudo semanage fcontext -a -t httpd_sys_content_t "/home/edesktop/Downloads/libression_cache(/.*)?"
    sudo restorecon -R -v /home/edesktop/Downloads/photos_to_print_copy
    sudo restorecon -R -v /home/edesktop/Downloads/libression_cache

    # Set additional required SELinux booleans
    sudo setsebool -P httpd_unified 1
    sudo setsebool -P httpd_can_network_connect 1
    sudo setsebool -P httpd_can_network_relay 1
    sudo setsebool -P httpd_anon_write 1
    sudo setsebool -P httpd_read_user_content 1
    sudo setsebool -P httpd_enable_homedirs 1
    sudo setsebool -P daemons_enable_cluster_mode 1

    ```

  - **Start and enable Nginx**:
    ```
    sudo systemctl enable nginx
    sudo systemctl start nginx
    sudo firewall-cmd --permanent --add-service=http
    sudo firewall-cmd --reload
    ```


- macos:
  - Set up https certs
    ```
    mkdir -p ~/certs
    cd ~/certs
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout nginx-selfsigned.key -out nginx-selfsigned.crt
    ```


  - brew install nginx --with-dav
  - brew install httpd
  - config /opt/homebrew/etc/nginx/nginx.conf:
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

            # Redirect HTTP to HTTPS
            return 301 https://$host$request_uri;
        }

        server {
            listen 443 ssl;
            server_name localhost;  # Use 'localhost' for local access

            ssl_certificate /Users/ernest/certs/nginx-selfsigned.crt;  # Path to your certificate
            ssl_certificate_key /Users/ernest/certs/nginx-selfsigned.key;  # Path to your private key

            auth_basic "Restricted Access";
            auth_basic_user_file /usr/local/etc/nginx/.htpasswd;

            # First libression_library directory
            location /dummy_photos/ {
                alias /Users/ernest/Downloads/dummy_photos/;

                dav_methods PUT DELETE MKCOL COPY MOVE;

                dav_access user:rw group:rw all:r;

                client_max_body_size 0;
                create_full_put_path on;

                autoindex on;
            }

            # Second libression_cache directory
            location /dummy_photos_cache/ {
                alias /Users/ernest/Downloads/dummy_photos_cache/;

                dav_methods PUT DELETE MKCOL COPY MOVE;

                dav_access user:rw group:rw all:r;

                client_max_body_size 0;
                create_full_put_path on;

                autoindex on;
            }

            # Secure path for presigned URLs
            location /secure/ {
                alias /Users/ernest/Downloads/dummy_photos/;  # Adjust this path if necessary

                auth_basic off;

                # Allow only GET requests for presigned URLs
                limit_except GET {
                    deny all;  # Deny all methods except GET
                }

                dav_access user:rw group:rw all:r;

                client_max_body_size 0;
                create_full_put_path off;

                autoindex off;  # Disable autoindexing for security
            }
        }
    }

    ```

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

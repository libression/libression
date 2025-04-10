FROM nginx:alpine

# Install required packages
RUN apk add --no-cache apache2-utils openssl

# Create directories and set permissions
RUN mkdir -p /var/www/webdav/libression_photos && \
    mkdir -p /var/www/webdav/readonly_libression_photos && \
    mkdir -p /var/www/webdav/libression_photos_cache && \
    mkdir -p /var/www/webdav/readonly_libression_photos_cache

# Generate SSL certificates
RUN openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout /etc/nginx/cert.key \
    -out /etc/nginx/cert.crt \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# Copy configuration files/scripts
COPY nginx_docker.conf /etc/nginx/nginx.conf
COPY webdav-docker-entrypoint.sh /usr/local/bin/

# Set permissions
RUN chmod +x /usr/local/bin/webdav-docker-entrypoint.sh \
    && chown -R nginx:nginx /var/www/webdav \
    && chmod -R 755 /var/www/webdav \
    && chmod 644 /etc/nginx/cert.key /etc/nginx/cert.crt

EXPOSE 443

ENTRYPOINT ["/usr/local/bin/webdav-docker-entrypoint.sh"]
CMD ["nginx", "-g", "daemon off;"]

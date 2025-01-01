FROM nginx:alpine

# Install required packages
RUN apk add --no-cache apache2-utils openssl

# Create directories and set permissions
RUN mkdir -p /var/www/webdav/dummy_photos && \
    mkdir -p /var/www/webdav/readonly_dummy_photos

# Copy configuration files/scripts
COPY sample_config/nginx_docker.conf /etc/nginx/nginx.conf
COPY webdav-docker-entrypoint.sh /usr/local/bin/

# Set permissions
RUN chmod +x /usr/local/bin/webdav-docker-entrypoint.sh \
    && chown -R nginx:nginx /var/www/webdav \
    && chmod -R 755 /var/www/webdav

EXPOSE 443

ENTRYPOINT ["webdav-docker-entrypoint.sh"]
CMD ["nginx", "-g", "daemon off;"]

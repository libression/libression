#!/bin/sh

# Generate self-signed certificates if they don't exist
if [ ! -f /etc/nginx/cert.crt ] || [ ! -f /etc/nginx/cert.key ]; then
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout /etc/nginx/cert.key \
        -out /etc/nginx/cert.crt \
        -subj "/C=US/ST=Test/L=Test/O=Test/CN=localhost"
fi

# Create .htpasswd file with runtime credentials
htpasswd -cb /etc/nginx/.htpasswd "${WEBDAV_USER}" "${WEBDAV_PASSWORD}"

# Create required directories with proper permissions
mkdir -p /var/www/webdav/dummy_photos
mkdir -p /var/www/webdav/readonly_dummy_photos
mkdir -p /var/www/webdav/dummy_photos_cache
mkdir -p /var/www/webdav/readonly_dummy_photos_cache

# Set ownership and permissions
chown -R nginx:nginx /var/www/webdav
chmod -R 755 /var/www/webdav  # Base permissions
find /var/www/webdav -type d -exec chmod u+w {} \;  # Add write permission for directories

# Ensure nginx can write to the directories
chmod 775 /var/www/webdav/dummy_photos
chmod 775 /var/www/webdav/readonly_dummy_photos
chmod 775 /var/www/webdav/dummy_photos_cache
chmod 775 /var/www/webdav/readonly_dummy_photos_cache

# Set SGID bit to maintain group ownership
find /var/www/webdav -type d -exec chmod g+s {} \;

# Set default ACLs to ensure new files/directories inherit correct permissions
if command -v setfacl >/dev/null 2>&1; then
    # Install acl if not present
    apk add --no-cache acl

    # Set default ACLs
    setfacl -R -d -m u::rwx,g::rx,o::rx /var/www/webdav
    setfacl -R -m u::rwx,g::rx,o::rx /var/www/webdav
fi

# Set umask for nginx process
echo "umask 022" >> /etc/profile

# Update nginx secure link key
sed -i "s/libression_secret_key/${NGINX_SECURE_LINK_KEY}/g" /etc/nginx/nginx.conf

# Execute CMD with correct umask
umask 022
exec "$@"

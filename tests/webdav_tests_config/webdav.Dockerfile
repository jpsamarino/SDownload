FROM bytemark/webdav

# Use basic auth
ENV AUTH_TYPE=Basic
ENV USERNAME=admin
ENV PASSWORD=admin

# Copy sample data
COPY tests/webdav_tests_config/www/ /var/lib/dav/data/

# Install tools to generate large files and ensure permissions
RUN apk add --no-cache coreutils && \
    dd if=/dev/urandom of=/var/lib/dav/data/large_file_1mb.bin bs=1M count=1 && \
    dd if=/dev/urandom of=/var/lib/dav/data/large_file_10mb.bin bs=10M count=1 && \
    chown -R www-data:www-data /var/lib/dav/data && \
    chmod -R 755 /var/lib/dav/data

EXPOSE 80

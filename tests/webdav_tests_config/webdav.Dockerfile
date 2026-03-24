FROM bytemark/webdav

# Use basic auth
ENV AUTH_TYPE=Basic
ENV USERNAME=admin
ENV PASSWORD=admin

# Copy sample data
COPY tests/webdav_tests_config/www/ /var/lib/dav/data/

# Ensure permissions (bytemark image uses davuser or equivalent)
RUN chown -R www-data:www-data /var/lib/dav/data && \
    chmod -R 755 /var/lib/dav/data

EXPOSE 80

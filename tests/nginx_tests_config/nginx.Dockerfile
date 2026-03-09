# Use Alpine-based Nginx for small footprint
FROM nginx:alpine

# Install tools: coreutils for dd, bash, openssl
RUN apk add --no-cache coreutils bash openssl

# Create directories for files, JSON, and SSL
RUN mkdir -p /usr/share/nginx/html/files \
  /usr/share/nginx/html/json \
  /etc/nginx/ssl

# Remove default site to avoid conflicts
RUN rm -f /etc/nginx/conf.d/default.conf

# Copy custom Nginx config
COPY tests/nginx_tests_config/nginx.conf /etc/nginx/nginx.conf
COPY tests/nginx_tests_config/conf.d/    /etc/nginx/conf.d/

# Generate random binary files from 100KB up to 10MB
RUN set -eux; \
  for size in 10k 100k 1M 10M; do \
  dd if=/dev/urandom of="/usr/share/nginx/html/files/file_${size}.bin" bs=${size} count=1 status=none; \
  done

# Generate a valid self-signed certificate (CN=localhost, 1 year)
RUN openssl req -x509 -nodes -newkey rsa:2048 \
  -keyout /etc/nginx/ssl/selfsigned.key \
  -out /etc/nginx/ssl/selfsigned.crt \
  -days 365 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost"

# Copy expired cert (valid for 1 day) generated on host
COPY tests/nginx_tests_config/ssl/expired.crt /etc/nginx/ssl/expired.crt
COPY tests/nginx_tests_config/ssl/expired.key /etc/nginx/ssl/expired.key

# Generate a sample JSON document (static for deterministic build)
RUN echo '{"message":"hello","id":123,"random":42, "links": ["http://localhost/default/file_100k.bin", "http://localhost/no_resume/file_1M.bin"]}' > /usr/share/nginx/html/json/data.json

# Copy static HTML pages for recursive listing tests
COPY tests/nginx_tests_config/www/ /usr/share/nginx/html/

# Expose HTTP and HTTPS ports
EXPOSE 80 443 8443

# RUN Nginx in foreground
CMD ["nginx", "-g", "daemon off;"]




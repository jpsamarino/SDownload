docker build -t sdownload_test_nginx -f tests/nginx_tests_config/nginx.Dockerfile .
docker run --rm -p 8080:80 sdownload_test_nginx

# curl -v http://localhost:8080/default/file_100k.bin
# curl -v http://localhost:8080/limited_speed/file_1M.bin

docker run --rm -p 8080:80 -p 8443:443 -p 9443:8443 sdownload_test_nginx
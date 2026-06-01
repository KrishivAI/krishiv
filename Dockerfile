FROM alpine:3.21
COPY dist/docker/krishiv /usr/local/bin/krishiv
COPY dist/docker/krishiv-operator /usr/local/bin/krishiv-operator
RUN chmod +x /usr/local/bin/krishiv /usr/local/bin/krishiv-operator
ENTRYPOINT ["/usr/local/bin/krishiv"]

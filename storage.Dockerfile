FROM alpine
LABEL maintainer="chilledgeek@gmail.com"

USER root
RUN adduser -D -g '' storage_user && \
    apk add wget && \
    wget -O minio https://dl.min.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2022-05-26T05-48-41Z && \
    chmod +x minio

RUN mkdir /mount_dir && \
    chown -R storage_user /mount_dir && \
    chmod u+rxw /mount_dir

USER storage_user
CMD [ "./minio",  "server", "/mount_dir", "--console-address", ":9001"]

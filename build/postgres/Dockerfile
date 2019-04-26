FROM postgres:11-alpine

RUN apk add --update git make gcc musl-dev

# https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.version111
# Use SHA that is compiled into RDS Postgres version 11.1
ARG wal2json_git_sha=9e962bad
RUN cd /opt && \
    git clone https://github.com/eulerto/wal2json.git && \
    cd wal2json && git checkout ${wal2json_git_sha} && \ 
    USE_PGXS=1 make && \
    USE_PGXS=1 make install

COPY build/postgres/postgresql.conf /etc/postgres/postgresql.conf
RUN chown -R postgres:postgres /etc/postgres

CMD ["postgres", "-c", "config_file=/etc/postgres/postgresql.conf"]

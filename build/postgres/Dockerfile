FROM postgres:11-alpine

RUN apk add --update git make gcc musl-dev

RUN cd /opt && \
    git clone https://github.com/eulerto/wal2json.git && \
    cd wal2json && \ 
    USE_PGXS=1 make && \
    USE_PGXS=1 make install

COPY build/postgres/postgresql.conf /etc/postgres/postgresql.conf
RUN chown -R postgres:postgres /etc/postgres

CMD ["postgres", "-c", "config_file=/etc/postgres/postgresql.conf"]

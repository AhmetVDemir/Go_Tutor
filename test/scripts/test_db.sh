#!/bin/bash

docker run --name postgres-test -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -p 6432:5432 -d postgres:latest

echo "PostgreSQL Başlatıldı !"
sleep 5

docker exec -it postgres-test psql -U postgres -d postgres -c "CREATE DATABASE productapp"
sleep 5

echo "productapp Veritabanı oluşturuldu"

docker exec -it postgres-test psql -U postgres -d productapp -c "
create table if not exists products(
    id bigserial not null primary key,
    name varchar(255) not null,
    price double precision not null,
    discount double precision,
    store varchar(255) not null
);"
sleep 5
echo "Ürün tablosu oluşturuldu."
drop table if exists test_table_$I;

create table test_table_$I
(
    id bigint not null,
    name varchar(128),
    ctime timestamp not null,
    primary key(id)         
);
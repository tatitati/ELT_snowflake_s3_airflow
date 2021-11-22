create table users(
    id integer not null auto_increment primary key,
    name varchar(100) not null,
    age integer not null,
    address varchar(100) not null,
    created_at datetime not null default current_timestamp(),
    updated_at datetime default null

);

create table orders(
    id integer not null auto_increment primary key,
    id_user integer not null,
    spent integer not null,
    status varchar(100) not null default 'processing',
    created_at datetime not null default current_timestamp(),
    updated_at datetime default null
);



insert into users(name, age, address)
values
    ('michael', 23, 'London st square 28'),
    ('john', 44, 'Madrid ST blabla 232'),
    ('raul', 65, 'Tokyo 2NW343');


insert into orders(id_user, spent)
values
    (1, 34),
    (2, 2300),
    (3, 1100);


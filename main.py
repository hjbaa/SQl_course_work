import random

import psycopg2
import time
from psycopg2 import errors
from mimesis import Person
from mimesis.schema import Field
from mimesis.locales import Locale
from mimesis import Address
from mimesis import builtins
from mimesis import Datetime

# Database Configuration
con = psycopg2.connect(
    database="...",
    user="...",
    password="...",
    host="...",
    port="..."
)
start_time = time.time()

custom_providers = (builtins.RussiaSpecProvider,)
_ = Field(Locale.RU, providers=custom_providers)

cur = con.cursor()


def drop_tables():
    cur.execute('''
        drop table if exists climbers_groups cascade;
        drop table if exists groups cascade;
        drop table if exists climbers cascade;
        drop table if exists mountains cascade;
        drop table if exists stop_points cascade;
        drop table if exists routes cascade;
    ''')


def create_tables():
    cur.execute('''
create table if not exists climbers(
    id bigserial primary key,
    full_name text not null,
    gender varchar(4),
    age int not null check (age > 17 and age < 75),
    weight int not null,
    phone_number varchar(18) not null unique,
    email text unique,
    blood_type varchar(3) not null
);

create table if not exists groups(
    id bigserial primary key,
    members_quantity int not null default 0 check ( members_quantity >= 0 ),
    leader_id bigint not null,

    foreign key(leader_id) references climbers(id)
);

create table if not exists climbers_groups(
    id bigserial primary key,
    group_id bigint not null,
    climber_id bigint not null,

    foreign key (group_id) references groups(id),
    foreign key (climber_id) references climbers(id),
    unique (group_id, climber_id)
);

create table if not exists mountains(
    id bigserial primary key,
    name text not null unique,
    height int not null check ( height > 500 ),
    country text not null,
    difficulty int not null check ( difficulty > 0 )
);

create table if not exists route(
    id bigserial primary key,
    mountain_id bigint not null,
    guides_full_name text not null,
    price integer not null,

    foreign key (mountain_id) references mountains(id)
);

create table if not exists ascents(
    id bigserial primary key,
    date_of_beginning timestamp,
    date_of_ending timestamp,
    success bool not null default false,
    success_percentage int not null default 0 check (success_percentage >= 0 and success_percentage <= 100),
    group_id bigint not null,
    route_id bigint not null,

    foreign key (group_id) references groups(id),
    foreign key (route_id) references route(id)
);

create table if not exists stop_points(
    id bigserial primary key,
    location text not null,
    route_id bigint not null,

    foreign key (route_id) references route(id)
);

CREATE OR REPLACE FUNCTION update_members_count()
RETURNS trigger AS $$
BEGIN
  IF (TG_OP = 'INSERT') THEN
    UPDATE groups SET members_quantity = groups.members_quantity + 1
      WHERE id = NEW.group_id;
    RETURN NEW;
  ELSIF (TG_OP = 'DELETE') THEN
    UPDATE groups SET members_quantity = groups.members_quantity - 1
      WHERE id = NEW.group_id;
    RETURN OLD;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS update_members_count_trg ON climbers_groups;
CREATE TRIGGER update_members_count_trg
AFTER INSERT OR DELETE
ON climbers_groups
FOR EACH ROW
EXECUTE PROCEDURE update_members_count();
''')


def insert_into_climbers(count):
    for i in range(count):
        person = Person(Locale.RU)
        try:
            cur.execute(f'''
                insert into climbers (full_name, gender, age, weight, phone_number, email, blood_type)
                VALUES (
                '{person.full_name()}',
                '{person.gender()}',
                {person.age(minimum=18, maximum=74)},
                {person.weight()},
                '{person.telephone()}',
                '{person.email(unique=True)}',
                '{person.blood_type()}'
              );'''
                        )
        except psycopg2.errors.UniqueViolation:
            con.rollback()
        else:
            con.commit()
            print(f"Record #{i} successfully inserted!", end="\n\n")


def insert_into_groups(count):
    for i in range(1000):
        temp = random.randint(1, count)
        cur.execute(f'''
        insert into groups (members_quantity, leader_id) values (
            0, {temp}
        );''')
        con.commit()


def insert_into_climbers_groups(count):
    for i in range(count):
        try:
            temp = random.randint(1, 1000)
            cur.execute(f'''
            insert into climbers_groups (group_id, climber_id) values (
            {temp}, {i + 0} 
            );''')
        except psycopg2.errors.ForeignKeyViolation:
            con.rollback()
        else:
            con.commit()


def insert_into_mountains():
    cur.execute(f'''
    insert into mountains (name, height, country, difficulty) values
    ('Бен-Невис', 1344, 'Великобритания', {random.randint(4, 7)}),
    ('Эверест', 8848, 'Непал', 10),
    ('Машербрум', 7821, 'Пакистан', {random.randint(7, 10)}),
    ('Свентокшиские горы', 612, 'Польша', 2),
    ('Барзиарлам', 2214, 'Россия', {random.randint(3, 7)}),
    ('Аконкагуа', 6960, 'Аргентина', {random.randint(7, 10)}),
    ('Верхний Зуб', 2178, 'Россия', {random.randint(3, 7)}),
    ('Вильдгалль', 3274, 'Италия', {random.randint(4, 6)}),
    ('Киппур', 757, 'Ирландия', {random.randint(1, 3)}),
    ('Лапа дьяволов', 2620, 'Канада', {random.randint(6, 10)});
    ''')
    con.commit()


def insert_into_route():
    for i in range(10):
        person = Person(Locale.RU)
        cur.execute(f'''
        insert into route (mountain_id, guides_full_name, price) values (
            {i + 1},
            '{person.full_name()}',
            {random.randint(1, 500000)}
        );''')

        con.commit()


def insert_into_stop_points():
    for i in range(10):
        for j in range(random.randint(1, 4)):
            address = Address(Locale.RU)
            cur.execute(f'''
            insert into stop_points (location, route_id) values (
                '{" ".join(list(map(str, list(address.coordinates().values()))))}',
                {i + 1}
            );''')


def insert_into_ascents(number_of_rows):
    for i in range(number_of_rows):
        date1 = Datetime(Locale.RU).datetime()
        date2 = Datetime(Locale.RU).datetime()

        percentage = random.randint(1, 100)
        if percentage > 50:
            success = 'True'
        else:
            success = 'False'

        if date1 > date2:
            date1, date2 = date2, date1

        cur.execute(f'''
        insert into ascents (date_of_beginning, date_of_ending, success, success_percentage, group_id, route_id) values (
            '{date1}',
            '{date2}',
            {success},
            {percentage},
            {random.randint(1, 1000)},
            {random.randint(1, 10)}
        );''')

        con.commit()
        print(f"Row #{i + 1} inserted!")


def update_ascents():
    for i in range(10):
        percentage = random.randint(1, 100)
        if percentage > 50:
            success = 'True'
        else:
            success = 'False'
        cur.execute(f'''
        update ascents
        set success = {success}, success_percentage = {percentage}
        where id = {i + 1}
        ''')

        con.commit()


n = 100000

# insert_into_climbers(20)
# insert_into_groups(n)
# insert_into_climbers_groups(n)
# insert_into_mountains()
# insert_into_route()
# insert_into_stop_points()
# insert_into_ascents()

# update_ascents()

insert_into_ascents(10000)

con.commit()

con.close()

print("\n\n--- %s seconds ---" % (time.time() - start_time))

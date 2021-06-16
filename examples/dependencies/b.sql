CREATE TABLE b(
    id CHAR(32) PRIMARY KEY -- gen: md5
  , id_a CHAR(32) NOT NULL -- gen: choose_from_list public.a.id
  , value BIGINT
);

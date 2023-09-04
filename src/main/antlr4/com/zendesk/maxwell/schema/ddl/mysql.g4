grammar mysql;

import mysql_literal_tokens,
       mysql_idents,
       mysql_alter_table,
       mysql_alter_database,
       mysql_create_database,
       mysql_create_table,
       mysql_drop,
       mysql_truncate,
       mysql_indices,
       mysql_rename,
       mysql_view;

parse: statement?
       EOF;

statement:
    alter_table
  | alter_view
  | alter_database
  | create_database
  | create_table
  | create_view
  | drop_database
  | drop_table
  | truncate_table
  | index_create
  | index_drop
  | drop_view
  | rename_table
  | BEGIN
  ;

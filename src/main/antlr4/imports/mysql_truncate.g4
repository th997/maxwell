grammar mysql_truncate;
import mysql_literal_tokens, mysql_idents;

truncate_table: TRUNCATE TABLE? table_name;

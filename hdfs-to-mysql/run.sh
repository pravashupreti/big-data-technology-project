#! /bin/bash

mysql -u root -pcloudera -e "CREATE DATABASE IF NOT EXISTS RedditComment;"
mysql -u root -pcloudera -e "CREATE TABLE IF NOT EXISTS CommentResultTable (comment_id VARCHAR(255), username VARCHAR(255), category VARCHAR(255), tags VARCHAR(255), timestamp DOUBLE );"
mysql -u root -pcloudera -e "CREATE TABLE IF NOT EXISTS CommentCountTable (category VARCHAR(255),comment_count INT );"

mysql -u root -pcloudera -e "truncate RedditComment.CommentResultTable;"
mysql -u root -pcloudera -e "truncate RedditComment.CommentCountTable;"

sqoop export --connect jdbc:mysql://localhost:3306/RedditComment --username root -P --table CommentResultTable --export-dir /user/cloudera/CommentResultTable --input-fields-terminated-by ';' --input-lines-terminated-by '\n' -m 1
sqoop export --connect jdbc:mysql://localhost:3306/RedditComment --username root -P --table CommentCountTable --export-dir /user/cloudera/CommentCountTable --input-fields-terminated-by ';' --input-lines-terminated-by '\n' -m 1
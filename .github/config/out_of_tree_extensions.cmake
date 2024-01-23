
################# MYSQL_SCANNER
# Note: tests for mysql_scanner are currently not run.
duckdb_extension_load(mysql_scanner
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb_mysql
        GIT_TAG f75007116b474cc842c26bae32e8645dbc9002c6
        APPLY_PATCHES
)

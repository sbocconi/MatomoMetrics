my_db=matomo_14_05_25
data_dir="../DataExports"

if ! mysql.server status
then
    mysql.server start
fi

if ! mariadb-check ${my_db}
then
    echo "Importing ${my_db}.sql"
    mariadb < ${data_dir}/${my_db}.sql
fi


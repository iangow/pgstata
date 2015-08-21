
## 1. Install iODBC

Using MacPorts:
```
sudo port install libiodbc
```

## 2. Get psqlODBC

```
wget https://ftp.postgresql.org/pub/odbc/versions/src/psqlodbc-09.03.0400.tar.gz
tar xzf psqlodbc-09.03.0400.tar.gz 
cd psqlodbc-09.03.0400
./configure --with-iodbc --enable-pthreads
make
sudo make install

```

## 3. Install ODBC administrator and set up DSN

Get [ODBC Administrator Tool for Mac OS X v1.0](https://support.apple.com/kb/DL895?locale=en_US).
Open ODBC Administrator and set up the driver:

![](driver.png?raw=true)

and then the "User DSN":

![](user_dsn.png?raw=true)


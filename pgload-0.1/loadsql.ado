program define loadsql
*! Load the output of an SQL file into Stata, version 1.4 (iandgow@gmail.com)
version 13.1
syntax using/, CONN(string)

#delimit;
tempname sqlfile exec line;

file open `sqlfile' using `"`using'"', read text;
file read `sqlfile' `line';

while r(eof)==0 {;
    local `exec' `"``exec'' ``line'' 
     "';
    file read `sqlfile' `line';
};

file close `sqlfile';

* display "`conn'";

pgload "`conn'" "``exec''", clear;
* pgload "``dsn''" "SELECT permno, date, abs(prc) AS prc FROM crsp.dsf LIMIT 10", clear;

end;

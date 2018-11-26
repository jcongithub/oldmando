Find top 10 largest file in a directory
>find /start/dir –type f –exec du{} \; | sort –rn | head -10

Find process which is connecting on a port 7500
>netstat –p | grep 7500

Rendezvous deamon status web page
>http:// noc5-mkv-dapp01.ny.jpmorgan.com:7580

vi search and replace
   search str forward : /str
   search str backward: ?str
   
   Replace first occurrence on the current line:   :s/old/new
   Replace all on the current line             :   :/old/new/g
   Replace between two lines                   :   :line1,line2s/old/new/g
   Replace in whole file                       :   :%s/old/new/g
   
   Move cursor to the end of the line   :  $
   MOve cursor to the begin of the line :  0(zero)


Find Linux last reboot time
>who -b
>last reboot | head -1

sed command to replace any thing before "ANY" in following line with "PB_UAT"
source.usdoutright_mt.records = ${USD_ENV}.ANY.USD1YSWAMJPM.mngeswap

sed 's/.*\(ANY.\)/PB_UAT.\1/'

Pass log4j configuration file from JVM parameter on PC
-Dlog4j.configuration=C:/abc/efg/log4j.xml

grep the line contains ip address
>grep '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}' addresses.txt


Find all ip address in a file
grep  '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}' *.log | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}'| sort -u


Find hostname for a list of IP addresses
>cat host.txt | nslookup | grep Name: | cut -d' ' -f5

check solaris version
>uname –a
>cat /etc/release


Find ANDefaultValue in gateway log. -n shows line number
>grep -o -n "{ANDefaultValue [0-9.]*}" 4_20110106.txt


Extracting line 12 from myfile.txt file
>sed -n 12p myfile.txt


Unix
grep command options
-i ignore case
-v not match give pattern
 

awk command
awk ‘{print $2, $1 }’ filename  -- print the first two fields in opposite order



find files and remove them,   c>find . –name *.class –exec rm ‘{}’ ;
Don’t forget the space between ‘{}’ and ;



Python Notes
Language: a multi-paradigm language supports object oriented, functional or imperative style.
Web development framework: Django, Pylons

Django Framework
Object-relation mapper
Automatic admin interface
Elegant URL design
Template system
Cache system
Internationalization


Install issue with [error: package directory '\django' does not exist.]
Download 'Django-0.96.tar.gz' or whatever the latest version into C:\1 directory - Unzip it - the directory 'C:\1\Django-0.96' got created - All from the command prompt: - Run: subst J: C:\1\Django-0.96 - Run: J: - Run: setup.py install - now it works flawlessly. - Run: C: - Run: subst J: /d 

Mysite superuser jcheng/jcheng



Linux
HTTP
Configuration directory: /etc/httpd/conf
Restart httpd: /sbin/service httpd restart

Linux/Unix
KDE ~ K Desktop Environment
The default the target for makefile - By defaut,it begins by processing the first target that does not begin with '.'.

Most application is by
make
make install clean

Install RPM package
RPM - Red Hat Package Manager.  Mandriva and SuSE also use RPM for software management.

x86
x86 is the CPU architeture family. These go along the lines of i286, i386, i486, i586, etc. where the x is used as a variable for the 2, 3, 4, 5, etc.

i586 is the same as a pentium and amd k1, i686 is the same as pentium II and amd k2, and so on. x86-64 stands for 64-bit processors like Athlon-64's and Opteron that operate off of the x86 family.

FTP anonymous user
user id: anonymous
password: normally will be any email address

Library on Linux/Unix
1. Static libraries .a file
2. Dynamic libraries (or Dynamic shared object DSO) .so file


Sent from my iPhone


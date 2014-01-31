##tt
*a token tester*

calculate the difference, intersection, or union on large newline delimited files

tt uses scalable bloom filters to quickly test the existence of a member in a set.

[Mac](https://github.com/jasonmoo/tt/raw/master/builds/tt-darwin-amd64.gz)
[Linux](https://github.com/jasonmoo/tt/raw/master/builds/tt-linux-amd64.gz)
[Windows](https://github.com/jasonmoo/tt/raw/master/builds/tt-windows-amd64.gz)

##Usage:

	./tt

	Usage: tt -[i,d,u] file1 file2[ file3..]
	  -d=false: calculate the difference
	  -i=false: calculate the intersection
	  -u=false: calculate the union

## Example

	wc -l /usr/share/dict/*
	      39 /usr/share/dict/README
	     150 /usr/share/dict/connectives
	    1308 /usr/share/dict/propernames
	  235886 /usr/share/dict/web2
	   76205 /usr/share/dict/web2a
	  235886 /usr/share/dict/words
	  549474 total

	# outputs for different actions on /usr/share/dict files

	./tt -u /usr/share/dict/{web2*,words} > /dev/null
	** Token Report **
	Tokens output:  308732
	Total time:  9.612525239s

	./tt -d /usr/share/dict/{web2*,words} > /dev/null
	** Token Report **
	Tokens output:  618234
	Total time:  18.742208443s

	./tt -i /usr/share/dict/{web2*,words} > /dev/null
	** Token Report **
	Tokens output:  4869
	Total time:  5.55820695s

	./tt -i /usr/share/dict/* > /dev/null
	** Token Report **
	Tokens output:  0
	Total time:  386.177523ms



[MIT License](https://github.vimeows.com/jason/tt/raw/master/LICENSE)

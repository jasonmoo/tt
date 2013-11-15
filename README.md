##tt
*a token tester*

calculate the difference, intersection, or union on large newline delimited files

tt uses scalable bloom filters to quickly test the existence of a member in a set.

[Mac](https://github.vimeows.com/jason/tt/raw/master/builds/tt-darwin-amd64.gz)
[Linux](https://github.vimeows.com/jason/tt/raw/master/builds/tt-linux-amd64.gz)
[Windows](https://github.vimeows.com/jason/tt/raw/master/builds/tt-windows-amd64.gz)

##Usage:

	./tt

	Usage: tt -[i,d,u] [-unique] file1 file2[ file3..]
	  -d=false: calculate the difference
	  -i=false: calculate the intersection
	  -u=false: calculate the union
	  -unique=false: output the unique set of the values


	# this will output the unique set of words from all
	# bundled dictionaries on a mac

	wc -l /usr/share/dict/*
	      39 /usr/share/dict/README
	     150 /usr/share/dict/connectives
	    1308 /usr/share/dict/propernames
	  235886 /usr/share/dict/web2
	   76205 /usr/share/dict/web2a
	  235886 /usr/share/dict/words
	  549474 total

	./tt -u /usr/share/dict/* | wc -l
	  312123

	real    0m1.116s
	user    0m0.592s
	sys     0m1.097s


[MIT License](https://github.vimeows.com/jason/tt/raw/master/LICENSE)

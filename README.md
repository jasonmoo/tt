##tt
*a token tester*

calculate the difference, intersection, or union on large newline delimited files

tt uses maps and optionally scalable bloom filters to quickly test the existence of a member in a set.  bloom filters provide a way to process files larger than the available memory on a machine with a performance penalty.  so we default to maps unless `-blooms` is greater than 0.

##Usage:

	./tt

	Usage: tt -[i,d,u] [-blooms N] file1 file2[ file3..]
	  -blooms=0: number of bloom filters to use (lossy/false positives)
	  -u=false: calculate the union
	  -d=false: calculate the difference
	  -i=false: calculate the intersection

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
	Tokens output:  312091
	Total time:  292.83033ms

	./tt -d /usr/share/dict/{web2*,words} > /dev/null
	** Token Report **
	Tokens output:  312091
	Total time:  691.094689ms

	./tt -i /usr/share/dict/{web2*,words} > /dev/null
	** Token Report **
	Tokens output:  0
	Total time:  541.864576ms

	./tt -i /usr/share/dict/* > /dev/null
	** Token Report **
	Tokens output:  0
	Total time:  386.177523ms


[MIT License](https://github.vimeows.com/jason/tt/raw/master/LICENSE)

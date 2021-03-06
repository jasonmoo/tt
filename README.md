##tt
*a token tester*

cli tool to calculate the difference, intersection, or union on large newline delimited files

tt uses maps and optionally scalable bloom filters to quickly test the existence of a member in a set.  bloom filters provide a way to process files larger than the memory consumed by the map implementation.

##Usage:

	jason@mba ~: go get github.com/jasonmoo/tt

	jason@mba ~: tt
	Usage: tt -[i,d,u] [-trim] [-match "regex"] [-capture "regex"] [-large [-estimated_lines N]] file1 file2[ file3..]
		-buffer_size=1048576: buffered io chunk size
		-capture="": only process captured data
		-d=false: calculate the difference
		-devnull=false: do not output tokens, just counts
		-estimated_lines=0: estimate used to size bloom filters (set this to avoid prescan)
		-i=false: calculate the intersection
		-large=false: use bloom filters for large data size (may be lossy)
		-match="": only process matching lines
		-trim=false: trim each line
		-u=false: calculate the union

## Example

	jason@mba ~: wc -l /usr/share/dict/*
	      39 /usr/share/dict/README
	     150 /usr/share/dict/connectives
	    1308 /usr/share/dict/propernames
	  235886 /usr/share/dict/web2
	   76205 /usr/share/dict/web2a
	  235886 /usr/share/dict/words
	  549474 total

	# outputs for different actions on /usr/share/dict files

	jason@mba ~: tt -u -devnull /usr/share/dict/{web2*,words}
		tt starting up
		** Token Report **
		Lines scanned:  547977
		Tokens emitted:  312091
		Time:  250.914739ms
	jason@mba ~: tt -d -devnull /usr/share/dict/{web2*,words}
		tt starting up
		** Token Report **
		Lines scanned:  547977
		Tokens emitted:  312091
		Time:  632.523386ms
	jason@mba ~: tt -i -devnull /usr/share/dict/{web2*,words}
		tt starting up
		** Token Report **
		Lines scanned:  547977
		Tokens emitted:  0
		Time:  501.008685ms
	jason@mba ~: tt -i -devnull /usr/share/dict/*
		tt starting up
		** Token Report **
		Lines scanned:  549474
		Tokens emitted:  0
		Time:  395.460469ms


[LICENSE](https://github.vimeows.com/jason/tt/raw/master/LICENSE)

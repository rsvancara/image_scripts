# Image conversion scripts

These are scripts I commonly use in my photography workflow.  


## batch-convert.py 

Converts RAW Sony ARW and Canon CR2 files to jpg using libraw and vips.  This application
is multithreaded using the python multiprocess module and multiprocess.  It can process
files very fast using libraw and vips.  JPEG images are highly optimized for size and quality
using the excellent vips library.



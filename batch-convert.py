#!/usr/bin/env python3

import logging
from rawkit.raw import Raw
from rawkit.options import WhiteBalance
import pyvips
import argparse
import os
import sys
import time
from multiprocessing import Process, Queue


logger = logging.getLogger('batch-raw-converter')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

image_queue = Queue()

def scanner(i, q, directory, destination):
    """ Scans the specified directory looking for CR2 files and ARW files """
    logger.info("Starting up scanner thread")

    logger.info("Walking directory {} ".format(directory))

    for root, dirs, files in os.walk(directory):

        for name in files:

            fullpath = os.path.join(root, name)

            basedir = root

            toplevel = basedir.split("/")[-1]

            filename, file_extension = os.path.splitext(fullpath)
            
            basename = filename.split("/")[-1]

            if file_extension == '.ARW' or file_extension == '.CR2':

                logger.info("Found raw image file: {}".format(fullpath))

                exportdir = basedir + "/" + destination

                if not os.path.exists(exportdir):

                    try:
                        logger.debug("Making export directory {}".format(exportdir))
                        os.mkdir(exportdir)

                    except Exception as e:

                        logger.error("Error creating export directory {}".format(str(e)))

                        continue

                logger.info("Adding item to queue")


                item = {"destfile": basedir + "/" + destination + "/" + toplevel + "_" + basename + ".jpeg",
                        "tempfile": basedir + "/" + destination + "/" + toplevel + "_" + basename + ".tiff",
                        "srcfile": fullpath
                        }

                q.put_nowait(item)
    

def worker(i, q):
    """ Worker for processing files """

    logger.info("Starting up worker thread {}".format(i))

    count = 0 

    while True:

        item = None

        try:

            #item = q.get(True,5)
            item = q.get(False)

        except Exception as e:
            logger.error("Error getting next value from the queue")

        # Check to see if we get an item, if not, then we start
        # the timer and then the thread ends after 15 seconds
        if item is None:

            logger.debug("No item is found, waiting five seconds")
            time.sleep(5)
            count += 1
            if count > 2:
                break

            continue

        count = 0 
        logger.info("Worker {} processing file {}".format(i, item['srcfile']))

        try:
            processFile(i, item["srcfile"], item['tempfile'], item["destfile"])
        except Exception as e:
            logger.debug("Error saving file {} with error {}".format(item["srcfile"],str(e)))
        

    logger.info("Thread {} is ending".format(i))


def processFile(thread, path,temp,dest):
    """ Process the file"""

    logger.debug("Workder {} Reading file {} with rawkit and converting to intermediate tiff".format(thread, path))

    # Convert Image from RAW to TIFF
    with Raw(path) as raw:
        raw.options.white_balance = WhiteBalance(camera=False, auto=True)
        raw.options.noise_threshold = 0.5
        raw.save(filename=temp)
        logger.debug("Writing file to destination {} ".format(temp))

    logger.debug("Worker {} pplying additional filters and converting to JPEG {}".format(thread, dest))

    # Use python vips
    try:
        if os.path.exists(temp):
            image = pyvips.Image.new_from_file(temp, access='sequential')

            mask = pyvips.Image.new_from_array([[-1, -1, -1],
                                                [-1, 16, -1],
                                                [-1, -1, -1]
                                                ], scale = 8)

            image = image.conv(mask, precision='integer')

            image.write_to_file(dest)

            os.unlink(temp)
    except Exception as e:

        logger.error("Error converting tiff {} to jpeg with error {}".format(path,str(e)))


def main():
    """ main function """

    parser = argparse.ArgumentParser(description="Convert sony ARW to jpeg images in a subfolder of your choosing")

    parser.add_argument('--directory', help="Directory to scan", required=True)
    parser.add_argument('--destination', help="Sub directory to place converted files", required = True)

    args = parser.parse_args()


    if not os.path.exists(args.directory):

        logger.error("Directory does not exist, please provide a valid directory")
        sys.exit(1)


    logger.info("Initializing scanner thread")
    scanner(0,image_queue, args.directory, args.destination)

    processes = []

    for i in range(2):

        wp = Process(target=worker, args=(i,image_queue,))

        logger.info("Initializing worker thread {} ".format(i))

        processes.append(wp)

    for wp in processes:
        wp.start()


    for wp in processes:    
        wp.join()


if __name__ == '__main__':
    
    main()

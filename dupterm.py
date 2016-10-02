'''
    dupterm.py - terminate duplicate files

    usage:
        python3 dupterm.py infolder outfolder [flags]

        no flags:
            gets all files from infolder and outputs only unique files to outfolder
            all original files from infolder are maintained there(unless -d is active)

        -n flag enables no IOjobs:
            just for testing and analyzing how much duplicates could you delete
            (no action is made to infolder nor outfolder)

        -d enables delete mode:
            deletes files that are duplicate(no action is made to outfolder)

    example:
        python3 /home/user/images . -d
            deletes all duplicate files from /home/user/images

        python3 /home/user/images /home/user/result
            moves only the unique files to /home/user/result,
            maintains /home/user/images intact

        python3 /home/user/images . -n
            shows the summary of duplicate files that you could delete,
            maintains /home/user/images and . intact
'''
from os import listdir
from os.path import isfile, join
import os
import hashlib
import sys
import time

file_dict = {}

VERSION = '0.1.1'

# globals
RW_ENABLED = None
FLAG_DELETE = None

# modification of http://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
def file_hash(fname, hash_obj, bs=4096):
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(bs), b""):
            hash_obj.update(chunk)
    return hash_obj.digest()

# main function
def main(args):
    print("dupterm v%s" % VERSION)
    RW_ENABLED = '-n' not in args
    FLAG_DELETE = '-d' in args

    infolder = args[1]
    outfolder = args[2]

    onlyfiles = [f for f in listdir(infolder) if isfile(join(infolder, f))]
    numjobs = len(onlyfiles)
    js_update_time = int(numjobs / 10) + 1
    cpu_jobs = 1
    print('hashing', numjobs, 'files')

    time_all_started = time.time()
    sum_all_times = 0
    jobs_per_sec = 0


    hashjob_st = time.time()

    for file in onlyfiles:
        fpth = join(infolder, file)

        time_job_start = time.time()
        filehash = file_hash(fpth, hashlib.sha256())
        time_job_end = time.time()

        if filehash not in file_dict:
            file_dict[filehash] = []

        file_dict[filehash].append(fpth)

        sum_all_times += (time_job_end - time_job_start)

        if cpu_jobs % js_update_time == 0:
            jobs_per_sec = cpu_jobs / sum_all_times

        sys.stdout.write("\r[%d/%d] %.2fcpujobs/sec" % (cpu_jobs, numjobs, jobs_per_sec))
        sys.stdout.flush()

        cpu_jobs += 1

    hashjob_end = time.time()

    cpu_per_sec_all = cpu_jobs / (hashjob_end - hashjob_st)

    print()

    duplicates = 0
    noduplicates = 0
    worked_files = 0
    io_jobs = 0
    iojobs_per_sec = 0
    iojobs_ttaken = 0

    dup_st = time.time()
    iojobs_upd_time = int(len(file_dict) / 10) + 1

    for fhash in file_dict:
        listfiles = file_dict[fhash]
        worked_files += 1

        iojob_st = time.time()
        if len(listfiles) > 1:
            duplicates += 1
            fpth = listfiles[0]

            sys.stdout.write("\r\rDUP %s out of %d files [%s]\n" % (fpth.replace(infolder, ''), len(listfiles),
                ', '.join([v.replace(infolder, '') for v in listfiles[1:]])))
            sys.stdout.flush()

            if RW_ENABLED:
                if FLAG_DELETE:
                    for other in listfiles[1:]:
                        os.remove(other)
                else:
                    io_jobs += 1
                    outpath = fpth.replace(infolder, outfolder)
                    with open(outpath, 'wb') as fout:
                        with open(fpth, 'rb') as fin:
                            fout.write(fin.read())
        else:
            # get first
            noduplicates += 1
            if RW_ENABLED and not FLAG_DELETE:
                fpth = listfiles[0]
                io_jobs += 1
                outpath = fpth.replace(infolder, outfolder)
                with open(outpath, 'wb') as fout:
                    with open(fpth, 'rb') as fin:
                        fout.write(fin.read())

        iojob_end = time.time()
        time_taken = (iojob_end - iojob_st)
        iojobs_ttaken += time_taken

        if io_jobs % iojobs_upd_time == 0:
            iojobs_per_sec = io_jobs / iojobs_ttaken

        sys.stdout.write('\r[%d/%d] %.2fiojobs/sec' % (
            worked_files, len(file_dict), iojobs_per_sec
        ))

        sys.stdout.flush()

    dup_end = time.time()

    io_per_sec_all = io_jobs / (dup_end - dup_st)

    print("""
Summary:
    %d files
    %d duplicates
    %d normal files
    %d unique files
    %d removed files

    %d CPU jobs
    %d IO jobs

    %.2f CPU jobs / sec
    %.2f IO jobs / sec
    """ % (len(onlyfiles), duplicates, noduplicates, len(file_dict),
        (len(onlyfiles)-len(file_dict)),
        cpu_jobs, io_jobs,
        cpu_per_sec_all, io_per_sec_all))

if __name__ == '__main__':
    main(sys.argv)

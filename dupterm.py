from os import listdir
from os.path import isfile, join
import hashlib
import sys
import time

file_dict = {}

RW_ENABLED = False

# modification of http://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
def file_hash(fname, hash_obj, bs=4096):
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(bs), b""):
            hash_obj.update(chunk)
    return hash_obj.digest()

def main(args):
    infolder = args[1]
    outfolder = args[2]

    onlyfiles = [f for f in listdir(infolder) if isfile(join(infolder, f))]
    numjobs = len(onlyfiles)
    cpu_jobs = 1
    print('hashing', numjobs, 'files')

    time_all_started = time.time()
    sum_all_times = 0
    jobs_per_sec = 0

    for file in onlyfiles:
        fpth = join(infolder, file)

        time_job_start = time.time()
        filehash = file_hash(fpth, hashlib.sha256())
        time_job_end = time.time()

        if filehash not in file_dict:
            file_dict[filehash] = []

        file_dict[filehash].append(fpth)

        sum_all_times += (time_job_end - time_job_start)

        if cpu_jobs % 200 == 0:
            jobs_per_sec = cpu_jobs / sum_all_times

        sys.stdout.write("\r[%d/%d] %.2fcpujobs/sec" % (cpu_jobs, numjobs, jobs_per_sec))
        sys.stdout.flush()

        cpu_jobs += 1

    print()

    duplicates = 0
    noduplicates = 0
    worked_files = 0
    io_jobs = 0

    for fhash in file_dict:
        listfiles = file_dict[fhash]
        worked_files += 1

        if len(listfiles) > 1:
            duplicates += 1

            if RW_ENABLED:
                fpth = listfiles[0]
                print("MAINTAINING FIRST ", fpth)
                io_jobs += 1
                outpath = fpth.replace(infolder, outfolder)
                with open(outpath, 'wb') as fout:
                    with open(fpth, 'rb') as fin:
                        fout.write(fin.read())
        else:
            # get first
            noduplicates += 1
            if RW_ENABLED:
                fpth = listfiles[0]
                io_jobs += 1
                outpath = fpth.replace(infolder, outfolder)
                with open(outpath, 'wb') as fout:
                    with open(fpth, 'rb') as fin:
                        fout.write(fin.read())

    print("""
Summary:
    %d files
    %d duplicates
    %d normal files
    %d unique files
    %d removed files

    %d CPU jobs
    %d IO jobs
    """ % (len(onlyfiles), duplicates, noduplicates, len(file_dict),
        (len(onlyfiles)-len(file_dict)),
        cpu_jobs, io_jobs))

if __name__ == '__main__':
    main(sys.argv)

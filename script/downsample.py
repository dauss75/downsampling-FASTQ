#!/usr/bin/python

import os
import sys
import subprocess
import time, datetime
import logging
import argparse
import re

def submit_multi_qsub(step_name, step_exe, step_args, step_nproc, step_runfolder, \
                      flag_dir, flags_exe, samples):
    job_list = []
    # static qsub variables
    step_args=" ".join(step_args)
    resources = "nodes=1:ppn=" + step_nproc
    walltime = "walltime=24:00:00"
    # job_name = step_name

    for i in samples:
        fastqr1, fastqr2 = i
        fastqname = os.path.basename(fastqr1)
        samplename = fastqname.split("_")[0]
        job_name = samplename
        step_args2 = step_args + " --fastqr1 " + fastqr1 + " --fastqr2 " + fastqr2

        cmd = ["/usr/local/bin/qsub",
               "-l", resources,
               "-l", walltime,
               "-j oe",
               "-N", job_name,
               "-r y -d", step_runfolder,
               "-F '", step_args2,"'",
                step_exe
               ]
        logging.info("downsample qsub: {}".format(" ".join(cmd)))
        try:
            p = subprocess.check_output(" ".join(cmd), stderr=subprocess.STDOUT, shell=True).decode('utf-8').rstrip()
            job_list.append(p)

        except subprocess.CalledProcessError:
            ts = time.time()
            timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            logging.error("Generating fastq files has errored out for {0} at {1}".format(step_runfolder, timestamp))

    logging.info("\n  Waiting for " + step_name + " jobs to complete...\n")

    flat_job_list = ':'.join(job_list)
    logging.info("\n  flat_job_list: " + flat_job_list + "...\n")

    wait_job_list = "depend=afterok:" + flat_job_list
    wait_resources = "nodes=1:ppn=1"
    wait_job_name = "wait_" + step_name + "_jobs"

    flag_args = ["--flag_folder",flag_dir,
                 "--flag_file",step_name]

    make_flag_args = " ".join(flag_args)
    logging.info("make_flag_args: {} ".format(make_flag_args))
    qsub_wait = ["/usr/local/bin/qsub",\
                 "-l",wait_resources,\
                 "-W",wait_job_list,\
                 "-d",step_runfolder,\
                 "-r y -j oe",\
                 "-o",flag_dir,\
                 "-N",wait_job_name,\
                 "-F '",make_flag_args,"'",\
                 flags_exe]
    logging.info("fastp qsub wait: {}".format(" ".join(qsub_wait)))
    qsub_wait_out = subprocess.check_output(" ".join(qsub_wait), stderr=subprocess.STDOUT, shell=True).decode('utf-8').rstrip()
    batch_completed = 0
    while batch_completed == 0:
      if os.path.isfile(flag_dir + "/" + step_name + ".done"):
        batch_completed = 1
      else:
        time.sleep(1)

def __main__():
    '''
    This code assumes to execute jobs via qsub  in a cluster
    command: ./downsample.py --input_dir {FASTQ_Input_Folder} --output_dir {Downsampled_FASTQ_Folder} --num_reads {number of reads (i.e., 10000000)}
    '''
    parser = argparse.ArgumentParser(description='downsample using seqtk')
    parser.add_argument('-i', '--input_dir', help="FASTQ input folder", required=True)
    parser.add_argument('-o', '--output_dir', help="downsampled FASTQ output folder", required=True)
    parser.add_argument('-n', '--num_reads', help="number of reads", required=True)
    args = parser.parse_args()
    input_dir = os.path.abspath(args.input_dir)
    output_dir = os.path.abspath(args.output_dir)
    num_reads = args.num_reads

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    ext_list=('fq.gz','fastq.gz')
    fastq_list = sorted([ f for f in os.listdir(input_dir) if os.path.isfile("/".join([input_dir, f])) and f.endswith(ext_list)])

    # Get List of paried fastq files
    fastq_pairs = []
    for fastq in fastq_list:
        if fastq not in fastq_pairs and "R1" in fastq:
            fastqr1 = input_dir + "/" + fastq
            fastqr2 = input_dir + "/" + re.sub("R1", "R2", fastq)

            if os.path.isfile(fastqr1) and os.path.isfile(fastqr2):
                fastq_pairs.append([fastqr1,fastqr2])

    flags_exe = "/isilon/RnD/tools/custom_script/downsample/flagMaker.py"
    seqtk_path = "/isilon/prodx/bcbio/tools/bin/seqtk"
    bgzip = "/isilon/prodx/bcbio/tools/bin/bgzip"
    step_name = "downsample"
    downsample_exe = "/isilon/RnD/tools/custom_script/downsample/ds_cmd.py"
    downsample_thread = '4'
    downsample_args = ["--input_dir", input_dir, "--output_dir", output_dir, "--seqtk_path", seqtk_path, "--num_reads", num_reads, \
                       "--threads", downsample_thread, "--bgzip", bgzip]

    log_folder = output_dir + '/logdir'
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    logF = os.path.abspath(log_folder) + '/log.txt'
    logging.basicConfig(filename=logF,filemode='w',level=logging.DEBUG, \
                        format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

    if not os.path.exists('{}/{}.done'.format(log_folder, step_name)):
        submit_multi_qsub(step_name, downsample_exe, downsample_args, downsample_thread, \
                      output_dir,log_folder, flags_exe, fastq_pairs)
    else:
        logging.info("{} is done...skipping...".format(step_name))



if __name__=="__main__": __main__()

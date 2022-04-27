#!/isilon/prodx/bcbio/anaconda/bin/python

import os, subprocess
import logging
import argparse
import time, datetime

def ds_run(seqtk_path, input_dir, output_dir, num_reads, fastqr1, fastqr2, threads, bgzip):
    fastq_out_r1=os.path.basename(fastqr1)
    fastq_out_r2=os.path.basename(fastqr2)
    out1=output_dir + "/" + fastq_out_r1
    out2=output_dir + "/" + fastq_out_r2
    cmd1 = " ".join([seqtk_path,"sample", "-s81", fastqr1, num_reads, "|", bgzip, "--threads", threads, ">", out1])
    cmd2 = " ".join([seqtk_path,"sample", "-s81", fastqr2, num_reads, "|", bgzip, "--threads", threads, ">", out2])
    cmd = ";".join([cmd1,cmd2])

    # logging.info("ebv cmd2 for {}: {}\n".format(sn, cmd2))
    out, error = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd=output_dir).communicate()
    if error:
        logging.error("An error occured: {error}".format(error=error))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='test')
    parser.add_argument('--seqtk_path', help='seqtk_path', required=True)
    parser.add_argument('--input_dir', help='input dir for FASTQ', required=True)
    parser.add_argument('--output_dir', help='output dir for downsampled FASTQ', required=True)
    parser.add_argument('--num_reads', help='number of reads to downsample', required=True)
    parser.add_argument('--threads', help='threads for compression', required=True)
    parser.add_argument('--bgzip', help='bgzip path', required=True)
    parser.add_argument('--fastqr1', help='FASTQ R1', required=True)
    parser.add_argument('--fastqr2', help='FASTQ R2', required=True)

    args = parser.parse_args()
    time.sleep(5)
    ds_run(args.seqtk_path, args.input_dir, args.output_dir, args.num_reads, args.fastqr1, args.fastqr2, args.threads, args.bgzip)

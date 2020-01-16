""" analysis_pipeline.py

This tool search a project against the clusterd spectra to get Confident PSMs and new PSMs

Usage:
  analysis_pipeline.py --project=<project_id>  --minsize=<minClusterSize> [--silent_op=<y|n>]
  analysis_pipeline.py (--help | --version )

Options:
  -p, --project=<project_id>       project_id to be processed.
  -s, --minsize=<minClusterSize>   minimum cluster size to be matched.
  -t, --silent_op=<y|n>            run in silent mode, with default option y|n, use yes for all
  -h, --help                       Print this help message.
  -v, --version                    Print the current version.

"""

import sys
import os
#sys.path.insert(0, "/code/py-venv/lib/python3.6/site-packages")
from docopt import docopt
import urllib.request
import json
import time
import logging
import configparser
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
#import phoenix_storage_access as phoenix
import mysql_storage_access as mysql_acc
from subprocess import Popen,PIPE,STDOUT
import  utils.mzident_reader as mzident_reader

#max number of parallel jobs
parallel_jobs = 1

#resultFile name with running status
result_file_name = "resultFiles.txt.started"

config = configparser.ConfigParser()
config.read("%s/config.ini"%(file_dir))

def get_result_files(project_id):
    # xmlfiles = glob.glob(project_id+ '/*.xml')
    result_file_path = project_id + "/" + result_file_name
    result_files = list()
    try:
        with open (result_file_path, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                items = line.split()
                result_files.append({"filename":items[0], "filetype":items[1]})
    except IOError:
        if project_id.startswith("E"):
            logging.info('resultFiles.txt.started does not exist for Enhancer Project %s! \nAbort pipeline.'%(project_id))
            print('resultFiles.txt.started does not exist for Enhancer Project %s! \nAbort pipeline.'%(project_id))
            sys.exit(0)

        logging.info('resultFiles.txt.started does not exist! Going to download data from PRIDE WebService')
        print('resultFiles.txt.started does not exist! Going to download data from PRIDE WebService')
        project_files_url = config.get("Urls","project_files_url")  + project_id
        try:
            with urllib.request.urlopen(project_files_url) as response:
                resp_str = response.read().decode('utf-8')
                json_obj = json.loads(resp_str)
                files = json_obj.get("list")

                for file in files:
                    if file["fileType"] == "RESULT":
                        if file["fileName"].endswith(".mzid") or file["fileName"].endswith(".mzid.gz") or \
                           file["fileName"].endswith(".mztab") or file["fileName"].endswith(".mztab.gz"):
                            result_files.append("%s\t%s"%(file["fileName"], "psm")) #mzid or mztab as psm
                        if file["fileName"].endswith(".xml") or file["fileName"].endswith(".xml.gz") :
                            result_files.append("%s\t%s"%(file["fileName"], "peaknpsm")) #pride xml as peaknpsm

                with open (result_file_path, 'w') as f:
                    for result_file in result_files:
                        f.write(result_file + "\n")
                    logging.info("Done of write result files to: " + result_file_path)
        except Exception as err:
            print(err)
            print("Failed to download result files from PRIDE WebService!")

    return result_files

def add_peak_file(project_id, ms_runs):
    """
    to find if the related peak file exists, by same name of ms_run, or read from the mzid file
    :param project_id:
    :param ms_runs:
    :return:
    """
    for ms_run in ms_runs:
        peakfile = ""
        filetype = ms_run.get('filetype')
        filename= ms_run.get('filename')
        psmfiletype = ms_run.get('psmfiletype')

        if os.path.exists(project_id + os.sep + ms_run['name'] + ".mgf"):
            peakfile = ms_run['name'] + ".mgf"
        elif os.path.exists(project_id + os.sep + ms_run['name'] + ".MGF"):
            peakfile = ms_run['name'] + ".MGF"
        elif os.path.exists(ms_run['name'] + ".mzML"):
            peakfile = ms_run['name'] + ".mzML"

        if psmfiletype == "pridexml" :  #use same file name
            peakfile = ms_run['filename']

        if psmfiletype == "mgf":#peaknpsm file type
            peakfile = ms_run['name'] + ".mgf"
            if os.path.exists(project_id + os.sep + ms_run['name'] + ".MGF"):
                peakfile = ms_run['name'] + ".MGF"


        #if no same name peak file found, and not psmnpeak file, retrieve peak file name from mzid file
        if peakfile == "" and psmfiletype == "mzid":
            if(os.path.exists(project_id + os.sep + filename)):
                (score_field, peakfile) = mzident_reader.get_scfield_peakfile(project_id + os.sep + filename)
                if not os.path.exists(project_id + os.sep + peakfile):
                    raise Exception("No same name peakfile, and the source peakfile %s from mzid file not exists"%(peakfile))

        if peakfile == "" : #still no peak file found
            raise Exception("No peak file exists for ms_run %s, we have looked for file name %s.mgf/mzML or the \
                                source file name in psm file"%(filename, ms_run['name']))

        ms_run['peakfile'] = peakfile
    return ms_runs

def get_ms_runs(result_files):
    """
    here we only get the file has psm
    :param result_files:
    :return:
    """
    ms_runs = list()
    for file in result_files:
        filetype = file.get("filetype")
        filename = file.get("filename")
        if filetype=="peak":  #don't consider the peak files as ms_run files
            continue
        ms_run_name = ""
        psmfiletype = ""
        if filename.lower().endswith(".xml.gz") :
            ms_run_name = filename[:-7]
            psmfiletype= "pridexml"
        elif filename.lower().endswith(".mgf.gz"):
            ms_run_name = filename[:-7]
            psmfiletype= "mgf"
        elif filename.lower().endswith(".mzid.gz"):
            ms_run_name = filename[:-8]
            psmfiletype= "mzid"
        # elif filename.lower().endswith(".mztab.gz"):
        #     ms_run_name = filename[:-9]
        #     psmfiletype= "mztab"
        elif filename.lower().endswith(".xml") :
            ms_run_name = filename[:-4]
            psmfiletype= "pridexml"
        elif filename.lower().endswith(".mgf"):
            ms_run_name = filename[:-4]
            psmfiletype= "mgf"
        elif filename.lower().endswith(".mzid"):
            ms_run_name = filename[:-5]
            psmfiletype= "mzid"
        # elif filename.lower().endswith(".mztab"):
        #     ms_run_name = filename[:-6]
        #     psmfiletype= "mztab"
        else:
            raise Exception("Filename: %s in resultFile does not end with .xml or .xml.gz or .mzid/.mztab or .mzid.gz/.mztab.gz" % (file))

        this_peakfile = ''
        if filetype == 'psm':
            for result_file in result_files:
                result_filename = result_file.get('filename')
                if result_filename == ms_run_name + '.mgf' or result_filename == ms_run_name + '.MGF' \
                        or result_filename == ms_run_name + '.mzML':
                    this_peakfile = result_filename
                    break
            if this_peakfile == '':
                print("this psm file: %s don't have peak file %s with same NAME"%(filename, this_peakfile))
                logging.error("this psm file: %s don't have peak file %s with same NAME"%(filename, this_peakfile))

        ms_runs.append({"name":ms_run_name, "psmfiletype":psmfiletype,
                        "peakfile":this_peakfile,
                        "filetype":filetype, "filename":filename})

    if len(ms_runs) < 1:
        raise Exception("There is no psm files in the file list")
    return ms_runs

def create_unzip_shell_files(project_id, result_files):
    if len(result_files) < 1:
        return
    unzip_file = project_id + "/unzip.sh"
    cd_shell_path = "cd $(dirname $([ -L $0 ] && readlink -f $0 || echo $0))\n"
    temp_index = parallel_jobs
    with open(unzip_file,"w") as f:
        f.write(cd_shell_path)
        for file in result_files:
            filename = file.get('filename')
            file_path = project_id  + "/" + filename
            if os.path.isfile(file_path) and filename.endswith(".gz"):
                temp_index -= 1
                if temp_index == 0:
                    temp_index = parallel_jobs
                    f.write("gzip -d \"" + filename + "\" ;\n")
                else:
                    f.write("gzip -d \"" + filename + "\" &\n")

            elif os.path.isfile(project_id  + "/" + filename[:-3]):
                continue #this file has been unzipped
            elif os.path.isfile(project_id  + "/" + filename):
                if (filename.lower().endswith(".xml") or
                    filename.lower().endswith(".mzid") or
                    filename.lower().endswith(".mgf") or
                    filename.lower().endswith(".mztab") or
                    filename.lower().endswith(".mzml") or
                    filename.lower().endswith(".mgf")
                ):
                    continue #the uploaded file is unzipped
            else:
                print("----\n" + filename + " is wrong in unzip step\n----\n")
                sys.exit()
                # raise Exception("result file %s is missing" % (file))

        print("done with creating unzip.sh")

def create_merge_shell_files(project_id, ms_runs, type):
    if len(ms_runs) < 1:
        logging.error("ms_runs is empty")
        return

    import_file = project_id + "/merge_%s_csv.sh"%type

    cd_shell_path = "cd $(dirname $([ -L $0 ] && readlink -f $0 || echo $0))\n"
    output_file = "%s_%s.csv"%(project_id, type)
    command_str = "cat *_%s.csv > %s"%(type, output_file)

    with open(import_file,"w") as f:
        f.write(cd_shell_path)
        f.write("rm %s_%s.csv \n" % (project_id, type))
        f.write("rm %s_%s.csv \n" % (project_id, type))
        f.write(command_str)

    print("done with creating merge.sh")
    logging.info("done with creating merge.sh")

def create_load_psms_peaks_to_csv_shell_files(project_id, ms_runs):
    if len(ms_runs) < 1:
        return

    shell_file = project_id + "/load_psms_peaks_to_csv.sh"

    cd_shell_path = "cd $(dirname $([ -L $0 ] && readlink -f $0 || echo $0))\n"
    pride_xml_converter= config.get("PipeLine", "pride_xml_converter")
    exe_path = os.path.abspath(os.path.dirname(__file__))
    mgf_converter= os.path.join(exe_path,"utils/mgf2csv.py")
    mzid_converter= os.path.join(exe_path, "utils/mzid2csv.py")
    mzml_converter= os.path.join(exe_path, "utils/mzml2csv.py")
    #todo will be added in the future   mztab_converter= config.get("PipeLine", "mztab_converter")

    pride_xml_command_str = "java -jar %s " + \
                   " -m -csv -p %s -i \"%s\" %s\n" #no importing to db here
#                   " -m -csv -ph -p %s -i \"%s\" %s\n"
    mzid_command_str = "python3 %s -p %s -i %s %s %s"
    mgf_peak_psm_command_str = "python3 %s -p %s -i %s --type peak_psm %s"
    mgf_peak_command_str = "python3 %s -p %s -i %s --type peak %s"
    mzml_peak_command_str = "python3 %s -p %s -i %s %s"

    with open(shell_file,"w") as f:
        f.write(cd_shell_path)
        temp_n_parallel_jobs = int(parallel_jobs/2)
        temp_index = temp_n_parallel_jobs
        for ms_run in ms_runs:
            f.write("rm %s_psm.csv \n" % (ms_run.get("name")))
            f.write("rm %s_spec.csv \n" % (ms_run.get("name")))
            psmfiletype =ms_run.get("psmfiletype")
            filetype =ms_run.get("filetype")

            if psmfiletype == "mztab":
                raise Exception("mztab is not supported right now, please wait for our upgrade")
            filename = ms_run['filename']
            if filename.endswith(".gz"):
                filename = filename[:-3]
            peakfile = ms_run['peakfile']
            peakfile_option = "--peakfile %s"%peakfile

            temp_index -= 1
            if temp_index == 0:
                temp_index = temp_n_parallel_jobs
                if psmfiletype == "pridexml":
                    f.write(pride_xml_command_str % (pride_xml_converter, project_id, filename, ';'))
                if psmfiletype == "mgf":
                    f.write(mgf_peak_psm_command_str %(mgf_converter, project_id, filename, ';'))
                if psmfiletype == "mzid":
                    f.write(mzid_command_str %(mzid_converter, project_id, filename, peakfile_option, ';'))
                    if peakfile.lower().endswith(".mgf"):
                        f.write(mgf_peak_command_str %(mgf_converter, project_id, peakfile, ';'))
                    elif peakfile.lower().endswith((".mzml")):
                        f.write(mzml_peak_command_str %(mzml_converter, project_id, peakfile, ';'))
            else:
                if psmfiletype == "pridexml":
                    f.write(pride_xml_command_str % (pride_xml_converter, project_id, filename, '&'))
                if psmfiletype == "mgf":
                    f.write(mgf_peak_psm_command_str %(mgf_converter, project_id, filename, '&'))
                if psmfiletype == "mzid":
                    f.write(mzid_command_str %(mzid_converter, project_id, filename, peakfile_option, '&'))
                    if peakfile.lower().endswith(".mgf"):
                        f.write(mgf_peak_command_str %(mgf_converter, project_id, peakfile, '&'))
                    elif peakfile.lower().endswith((".mzml")):
                        f.write(mzml_peak_command_str %(mzml_converter, project_id, peakfile, '&'))

        logging.info("done with creating load_psms_peaks_to_csv.sh")
        print("done with creating load_psms_peaks_to_csv.sh")

def create_convert_shell_files(project_id, ms_runs):
    if len(ms_runs) < 1:
        return
    convert_file = project_id + "/msconvert.sh"
    cd_shell_path = "cd $(dirname $([ -L $0 ] && readlink -f $0 || echo $0))\n"
    with open(convert_file,"w") as f:
        f.write(cd_shell_path)
        temp_index = parallel_jobs
        for ms_run in ms_runs:
            peakfile = ms_run.get('peakfile')
            if peakfile.endswith(".mgf") or peakfile.endswith(".MGF"):
                temp_index -= 1
                if temp_index == 0:
                    temp_index = parallel_jobs
                    f.write("%s \"%s\" --mzML -o %s ;\n" % (config.get("PipeLine","msconvert"), peakfile, "./"))
                else:
                    f.write("%s \"%s\" --mzML -o %s &\n" % (config.get("PipeLine","msconvert"), peakfile, "./"))
        print("Done with creating msconvert.sh")

def create_spectrast_shell_files(project_id, ms_runs):
    if len(ms_runs) < 1:
        return
    spectrast_search_file = project_id + "/spectrast_search.sh"
    cd_shell_path = "cd $(dirname $([ -L $0 ] && readlink -f $0 || echo $0))\n"
    with open(spectrast_search_file,"w") as f:
        f.write(cd_shell_path)
        temp_index = parallel_jobs
        spectrast = config.get("PipeLine","spectrast")
        speclib_file = config.get("PipeLine","speclib_file")
        for ms_run in ms_runs:
            mzml_file_name = "%s.mzML" % (ms_run.get('name'))
            temp_index -= 1
            if temp_index == 0:
                temp_index = parallel_jobs
                f.write("%s -sL %s\
                    \"%s\" ;\n" %(spectrast, speclib_file, mzml_file_name))
            else:
                f.write("%s -sL %s\
                    \"%s\" &\n" %(spectrast, speclib_file, mzml_file_name))
        print("done with creating spectrast_search.sh")

def main():
    arguments = docopt(__doc__, version='analysis_pipeline.py 1.0 BETA')
    project_id = arguments['--project'] or arguments['-p']
    min_cluster_size = arguments['--minsize'] or arguments['-s']
    min_cluster_size = int(min_cluster_size)
    is_silent = False
    if arguments.get('--silent_op', False) or arguments.get('-t', False):
        is_silent = True
        silent_op = arguments['--silent_op'] or arguments['-t']
    logging.basicConfig(filename="%s_pipeline.log"%project_id, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
    logging.info("Start to analysis (pipeline) project: " + project_id)

    result_files = get_result_files(project_id)
    logging.info("Get %d files from resultFiles.txt for project %s: " %(len(result_files), project_id))
    logging.info(result_files)

    ms_runs = get_ms_runs(result_files)
    ms_runs = add_peak_file(project_id, ms_runs)

    logging.info("Get %d msrun from resultFiles.txt for project %s: " %(len(ms_runs), project_id))
    logging.info(ms_runs)

    print(project_id)
    if not project_id.startswith("P"):
        # phoenix.upsert_analysis_status(project_id, 'started', 'localhost')
        mysql_acc.upsert_analysis_status(project_id, 'started')

    #unzip the gz files
    unzip_file = project_id + "/unzip.sh"
    redo = ''
    if is_silent:
        redo = silent_op
    if os.path.isfile(unzip_file):
        while redo != 'y' and redo != 'n':
            redo = input('the unzip.sh is already there, do you really want to redo the unzip process? y/n')
    if redo == 'y' or redo == '':
        create_unzip_shell_files(project_id, result_files)
        start = time.time()
        print("==--Starting unzip--==:\n")
        logging.info("==--Starting unzip--==:\n")
        output = os.popen('sh '+ project_id + "/unzip.sh").readlines()
        end = time.time()
        print(''.join(output) + "\n")
        logging.info(''.join(output) + "\n")
        logging.info("unziping take time %d seconds"%(end - start))

    #retrieve psms and peaks to csv file
    load_psms_peaks_to_csv_shell = project_id + "/load_psm_peaks_to_csv.sh"
    redo = ''
    if is_silent:
        redo = silent_op
    if os.path.isfile(load_psms_peaks_to_csv_shell):
        while redo != 'y' and redo != 'n':
            redo = input('the load_psms_peaks_to_csv.sh is already there, do you really want to redo the load to csv process? y/n')
    if redo == 'y' or redo == '':
        create_load_psms_peaks_to_csv_shell_files(project_id, ms_runs)
        start = time.time()
        print("==--Starting convert spectra & psm files --==:\n")
        logging.info("==--Starting convert spectra & psm files --==:\n")
        output1 = os.popen('sh '+ project_id + "/load_psms_peaks_to_csv.sh").readlines()
        end = time.time()
        logging.info("convert to spectra/psm csv take time %d seconds"%(end - start))
    #convert the peak files to mzml
    convert_to_mzml_file = project_id + "/msconvert.sh"
    redo = ''
    if is_silent:
        redo = silent_op
    if os.path.isfile(convert_to_mzml_file):
        while redo != 'y' and redo != 'n':
            redo = input('the msconvert.sh is already there, do you really want to redo the msconvert process? y/n')
    if redo == 'y' or redo == '':
        create_convert_shell_files(project_id, ms_runs)
        start = time.time()
        logging.info("==--Starting msconvert--==:\n" )
        output = os.popen('sh '+ project_id + "/msconvert.sh").readlines()
        end = time.time()
        print( ''.join(output) + "\n")
        logging.info(''.join(output) + "\n")
        logging.info("msconvert take time %d seconds"%(end - start))


    spectrast_search_file = project_id + "/spectrast_search.sh"
    redo = ''
    if is_silent:
        redo = silent_op
    if os.path.isfile(spectrast_search_file):
        while redo != 'y' and redo != 'n':
            redo = input('the spectrast_search.sh is already there, do you really want to redo the spectrast search process? y/n')
    if redo == 'y' or redo == '':
        create_spectrast_shell_files(project_id, ms_runs)
        start = time.time()
        print("==--Starting start spectrast search--==:\n")
        logging.info("==--Starting spectrast search--==:\n")
        output = os.popen('sh '+ project_id + "/spectrast_search.sh").readlines()
        end = time.time()
        print(''.join(output) + "\n")
        logging.info(''.join(output) + "\n")
        logging.info("spectrast searching take time %d seconds"%(end - start))
    #
    print("==--Starting enhancer analyze --==:\n")
    logging.info("==--Starting enhancer analyze --==:\n")
    start = time.time()
    file_dir = os.path.dirname(__file__)
    out = Popen(["python3", "%s/enhancer_analyze.py"%(file_dir), "-p", str(project_id), "-s", str(min_cluster_size)],  stdout=PIPE, stderr=STDOUT)
    end = time.time()
    stdoutput = str(out.communicate()[0])
    stdoutput = stdoutput.replace('\\n', "\n")
    return_code = out.returncode
    print( stdoutput )
    logging.info(stdoutput)
    logging.info("enhancer analyzing take time %d seconds"%(end - start))

    if return_code == 0:
        #phoenix.upsert_analysis_status(project_id, 'finished', 'localhost')
        if not project_id.lower().startswith('p'):
            mysql_acc.upsert_analysis_status(project_id, 'finished')
        os.rename(project_id + "/" + result_file_name, project_id + "/" + result_file_name[:-8])
    else:
        logging.info("This analysis %s is wrong"%project_id)
        #phoenix.upsert_analysis_status(project_id, 'finished_with_error', 'localhost')
        mysql_acc.upsert_analysis_status(project_id, 'finished_with_error')
if __name__ == "__main__":
    main()



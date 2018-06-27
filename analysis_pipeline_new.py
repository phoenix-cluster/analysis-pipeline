""" analysis_pipeline.py

This tool import the scan number --> ClusterUniID map to MySQL DB

Usage:
  analysis_pipeline.py --project =<project_id>
  analysis_pipeline.py (--help | --version)

Options:
  -p, --project=<project_id>        project_id to be processed.
  -h, --help                       Print this help message.
  -v, --version                    Print the current version.

"""

import sys
import os,glob
from docopt import docopt
import urllib.request
import json
import time
import logging

#max number of parallel jobs
parallel_jobs = 20

def get_result_files(project_id):
    # xmlfiles = glob.glob(project_id+ '/*.xml')
    result_file_path = project_id + "/resultFiles.txt"
    result_files = list()
    try:
        with open (result_file_path, 'r') as f:
            for filename in f.readlines():
                result_files.append(filename.strip())
    except IOError:
        print('resultFiles.txt does not exist! Going to download data from PRIDE WebService')
        project_files_url = "https://www.ebi.ac.uk:443/pride/ws/archive/file/list/project/%s" % (project_id)
        try:
            with urllib.request.urlopen(project_files_url) as response:
                resp_str = response.read().decode('utf-8')
                json_obj = json.loads(resp_str)
                files = json_obj.get("list")

                for file in files:
                    if file["fileType"] == "RESULT":
                        result_files.append(file["fileName"])

                with open (result_file_path, 'w') as f:
                    for result_file in result_files:
                        f.write(result_file + "\n")
        except Exception as err:
            print(err)
            print("Failed to download result files from PRIDE WebService!")

    return result_files

def get_ms_run_names(result_files):
    ms_run_names = []
    for file in result_files:
        if file.lower().endswith(".xml.gz"):
            ms_run_names.append(file[:-7])
        else:
            raise Exception("Something wrong with the result file %s" % (file))
    return (ms_run_names)

def create_unzip_shell_files(project_id, result_files):
    if len(result_files) < 1:
        return
    unzip_file = project_id + "/unzip.sh"
    cd_shell_path = "cd $(dirname $([ -L $0 ] && readlink -f $0 || echo $0))\n"
    temp_index = parallel_jobs
    with open(unzip_file,"w") as f:
        f.write(cd_shell_path)
        for file in result_files:
            if os.path.isfile(project_id  + "/" + file):
                temp_index -= 1
                if temp_index == 0:
                    temp_index = parallel_jobs
                    f.write("gzip -d \"" + file + "\" ;\n")
                else:
                    f.write("gzip -d \"" + file + "\" &\n")

            elif os.path.isfile(project_id  + "/" + file[:-3]):
                continue #this file has been unzipped
            else:
                print("----\n" + file[:-3] + "\n----\n")
                sys.exit()
                # raise Exception("result file %s is missing" % (file))

        print("done with creating unzip.sh")

def create_import_shell_files(project_id, ms_run_names):
    if len(ms_run_names) < 1:
        return

    import_file = project_id + "/import_to_phoenix.sh"

    cd_shell_path = "cd $(dirname $([ -L $0 ] && readlink -f $0 || echo $0))\n"
    command_str = "java -jar /home/ubuntu/mingze/tools/pridexml-to-phoenix/target/pridexml-to-phoenix-1.0-SNAPSHOT.jar " + \
                   " -m -csv -ph -p %s -i \"%s\" %s\n"

    with open(import_file,"w") as f:
        f.write(cd_shell_path)
        f.write("rm %s_psm.csv \n" % (project_id))
        f.write("rm %s_spec.csv \n" % (project_id))
        temp_n_parallel_jobs = int(parallel_jobs/5)
        temp_index = temp_n_parallel_jobs
        for ms_run_name in ms_run_names:
            file_name = ("%s.xml" % (ms_run_name))
            temp_index -= 1
            if temp_index == 0:
                temp_index = temp_n_parallel_jobs
                f.write(command_str % (project_id, file_name, ';'))
            else:
                f.write(command_str % (project_id, file_name, '&'))

        print("done with creating import_to_phoenix.sh")

def create_convert_shell_files(project_id, ms_run_names):
    if len(ms_run_names) < 1:
        return
    convert_file = project_id + "/msconvert.sh"
    cd_shell_path = "cd $(dirname $([ -L $0 ] && readlink -f $0 || echo $0))\n"
    with open(convert_file,"w") as f:
        f.write(cd_shell_path)
        temp_index = parallel_jobs
        for ms_run_name in ms_run_names:
            file_name = ("%s.mgf" % (ms_run_name))
            temp_index -= 1
            if temp_index == 0:
                temp_index = parallel_jobs
                f.write("msconvert \"%s\" --mzML -o %s ;\n" % (file_name, "./"))
            else:
                f.write("msconvert \"%s\" --mzML -o %s &\n" % (file_name, "./"))
        print("Done with creating msconvert.sh")

def create_spectrast_shell_files(project_id, ms_run_names):
    if len(ms_run_names) < 1:
        return
    spectrast_search_file = project_id + "/spectrast_search.sh"
    cd_shell_path = "cd $(dirname $([ -L $0 ] && readlink -f $0 || echo $0))\n"
    with open(spectrast_search_file,"w") as f:
        f.write(cd_shell_path)
        temp_index = parallel_jobs
        for ms_run_name in ms_run_names:
            file_name = "%s.mzML" % (ms_run_name)
            temp_index -= 1
            if temp_index == 0:
                temp_index = parallel_jobs
                f.write("spectrast -sL /home/ubuntu/mingze/spec_lib_searching/201504-spec-lib-nofilter/201504_nofil_min5.splib\
                    \"%s\" ;\n" %(file_name))
            else:
                f.write("spectrast -sL /home/ubuntu/mingze/spec_lib_searching/201504-spec-lib-nofilter/201504_nofil_min5.splib\
                    \"%s\" &\n" %(file_name))
        print("done with creating spectrast_search.sh")

def main():
    arguments = docopt(__doc__, version='analysis_pipeline.py 1.0 BETA')
    project_id = arguments['--project'] or arguments['-p']
    print(project_id)
    logging.basicConfig(filename="%s_pipeline.log"%project_id, level=logging.INFO)
    logging.info("Start to analysis (pipeline) project: " + project_id)

    result_files = get_result_files(project_id)
    ms_run_names = get_ms_run_names(result_files)


    unzip_file = project_id + "/unzip.sh"
    redo = ''
    if os.path.isfile(unzip_file):
        while redo != 'y' and redo != 'n':
            redo = input('the unzip.sh is already there, do you really want to redo the unzip process? y/n')
    if redo == 'y' or redo == '':
        create_unzip_shell_files(project_id, result_files)
        start = time.time()
        output = os.popen('sh '+ project_id + "/unzip.sh").readlines()
        end = time.time()
        print("==--unzip--==:\n" + ''.join(output) + "\n")
        logging.info("==--unzip--==:\n" + ''.join(output) + "\n")
        logging.info("unziping take time %d seconds"%(end - start))


    import_file = project_id + "/import_to_phoenix.sh"
    redo = ''
    if os.path.isfile(import_file):
        while redo != 'y' and redo != 'n':
            redo = input('the import_to_phoenix.sh is already there, do you really want to redo the import process? y/n')
    if redo == 'y' or redo == '':
        create_import_shell_files(project_id, ms_run_names)
        start = time.time()
        output = os.popen('sh '+ project_id + "/import_to_phoenix.sh").readlines()
        end = time.time()
        print("==--import--==:\n" + ''.join(output) + "\n")
        logging.info("==--import--==:\n" + ''.join(output) + "\n")
        logging.info("importing to phoenix/mgf take time %d seconds"%(end - start))

    convert_file = project_id + "/msconvert.sh"
    redo = ''
    if os.path.isfile(convert_file):
        while redo != 'y' and redo != 'n':
            redo = input('the msconvert.sh is already there, do you really want to redo the msconvert process? y/n')
    if redo == 'y' or redo == '':
        create_convert_shell_files(project_id, ms_run_names)
        start = time.time()
        output = os.popen('sh '+ project_id + "/msconvert.sh").readlines()
        end = time.time()
        print("==--msconvert--==:\n" + ''.join(output) + "\n")
        logging.info("==--msconvert--==:\n" + ''.join(output) + "\n")
        logging.info("msconvert take time %d seconds"%(end - start))


    spectrast_search_file = project_id + "/spectrast_search.sh"
    redo = ''
    if os.path.isfile(spectrast_search_file):
        while redo != 'y' and redo != 'n':
            redo = input('the spectrast_search.sh is already there, do you really want to redo the spectrast search process? y/n')
    if redo == 'y' or redo == '':
        create_spectrast_shell_files(project_id, ms_run_names)
        start = time.time()
        output = os.popen('sh '+ project_id + "/spectrast_search.sh").readlines()
        end = time.time()
        print("==--spectrast search--==:\n" + ''.join(output) + "\n")
        logging.info("==--spectrast search--==:\n" + ''.join(output) + "\n")
        logging.info("spectrast searching take time %d seconds"%(end - start))
    #
    start = time.time()
    output = os.popen("/home/ubuntu/mingze/tools/spectra-library-analysis/enhancer_analyze.py -p %s" % (project_id)).readlines()
    end = time.time()
    print("==--enhancer analyze --==:\n" + ''.join(output) + "\n")
    logging.info("==--enhancer analyze --==:\n" + ''.join(output) + "\n")
    logging.info("enhancer analyzing take time %d seconds"%(end - start))

if __name__ == "__main__":
    main()

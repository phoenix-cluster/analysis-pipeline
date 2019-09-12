import os, sys, time
sys.path.insert(0, "./venv/lib/python3.4/site-packages")

from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
import configparser

file_dir = os.path.abspath('.')
sys.path.append(file_dir)
import mysql_storage_access as mysql_acc

config = configparser.ConfigParser()
config.read("%s/config.ini"%(file_dir))


def after_request(response):
  # response.headers.add('Access-Control-Allow-Origin', 'http://192.168.6.20:4201')
  response.headers.add('Access-Control-Allow-Origin', 'http://enhancer.ncpsb.org')
  # response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,undefined')
  response.headers.add('Access-Control-Allow-Headers', '*, undefined, accessionId, token, analysisId')
  # response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  response.headers.add('Access-Control-Allow-Credentials', 'true')
  return response

app = Flask(__name__)
app.after_request(after_request)
api = Api(app)


# File upload
#
class FileUpload(Resource):
    UPLOAD_FOLDER = config.get("Web", "upload_dir")
    ALLOWED_EXTENSIONS = set(['xml','mzid','mztab', 'mzML', 'mgf', 'MGF', 'gz'])
    parser = reqparse.RequestParser()
    parser.add_argument('analysisId')
    parser.add_argument('accessionId')
    parser.add_argument('token')
    parser.add_argument('file', type=FileStorage, location='files', action='append')

    def allowed_file(self,filename):
        return '.' in filename and \
               filename.rsplit('.', 1)[1].lower() in self.ALLOWED_EXTENSIONS

    def post(self):
        data = self.parser.parse_args()
        uploaded_files = data['file']
        accession_id = request.headers['accessionId']
        token = request.headers['token']
        id = int(request.headers['analysisId'])
        if not accession_id:
            return{'message':"None accessionId", 'status':'error'}

        analysis_job = mysql_acc.get_analysis_job(id)
        if analysis_job.get('token') != token:
            return {
                    'data':'',
                    'message':'provided token is not equal to database, please input the right token for %s'%accession_id,
                    'status':'error'
                    }
        if len(uploaded_files) < 1:
            return {
                    'data':'',
                    'message':'No file found',
                    'status':'error'
                    }

        month = time.strftime("%Y%m", time.localtime())
        date = time.strftime("%Y%m%d", time.localtime())
        project_path = os.path.join(self.UPLOAD_FOLDER, month, accession_id)
        print("start to make project path")
        if not os.path.exists(project_path):
            print("path not exist, making %s"%project_path)
            os.makedirs(project_path)
        mysql_acc.update_analysis_job(id, project_path, date, 0, accession_id)
        failed_files = list()
        succ_files = list()
        for uploaded_file in uploaded_files:
            filename = uploaded_file.filename
            if filename == '':
                return {"message":"file name is null for %s"%uploaded_file, 'status':'error'}
            if self.allowed_file(filename):
                filename = secure_filename(filename)
                uploaded_file.save(os.path.join(project_path, filename))
                succ_files.append(filename)
            else:
                failed_files.append(filename)
        mysql_acc.update_analysis_job_status(id, 'uploading')
        if len(failed_files) > 1 :
            if len(succ_files) < 1 :
                return{
                    'message':"all files failed to upload because of format is not supported %s"%str(failed_files),
                    'status':'error'
                }
            else:
                return{
                    'message':"part files failed to upload because of format is not supported %s "%str(failed_files) +
                               "part files succeed %s" %str(succ_files),
                    'status':'part-error'
                }

        return {
                        'message':'all files uploaded',
                        'status':'success'
               }


# confirm uploaded file
#
class FileConfirm(Resource):

    RESULT_FILE_NAME = "resultFiles.txt"

    def is_file_list_correct(self, result_file_list, file_dir):
        nonexist_files = list()
        for file in result_file_list.get('fileList'):
            filename = file.get('fileName')
            # filetype = file.get('fileType')
            if not os.path.isfile(os.path.join(file_dir, filename)) or not os.path.getsize(os.path.join(file_dir, filename)):
                nonexist_files.append(filename)
        return nonexist_files

    def write_to_result_file(self, file_dir, result_file_list):
        with open(os.path.join(file_dir, self.RESULT_FILE_NAME), 'w') as f:
            for file_item in result_file_list.get("fileList"):
                filename = file_item.get('fileName')
                filetype = file_item.get('fileType')
                f.write("%s\t\t%s"%(filename, filetype))


    def post(self):
        json_data = request.get_json(force=True)
        if not request.headers['analysisId']:
            return{'message':"None analysis Id", 'status':'error'}
        analysis_id = int(request.headers['analysisId'])
        result_file_list = json_data
        analysis_job = mysql_acc.get_analysis_job(analysis_id);
        nonexist_files = self.is_file_list_correct(result_file_list, analysis_job.get('file_path'))
        message = {'id':analysis_id, 'message':'', 'status':''}
        if len(nonexist_files) < 1:
            print("you got %d  files: '%s' in AnalysisJob E%06d" % (int(result_file_list.get('fileListLength')), str(result_file_list) , analysis_id))
            message['status'] = "success"
            status, file_dir = mysql_acc.get_status_and_file_path(analysis_id)
            self.write_to_result_file(file_dir, result_file_list)
            mysql_acc.update_analysis_job_status(analysis_id, "uploaded")
        else:
            message['status'] = "error"
            message['message'] = "The file list is not as same as in the web server. " + \
                                 "These files don't exist or is empty: %s"%str(nonexist_files)
        return message


# Analysis Job apis
#
class DoAnalysis(Resource):

    def __is_analysis_started(self, status):
        if (status.lower() == "started" or status.lower() == "finished" or status.lower() == "finished_with_error"):
            return True
        else:
            return False

    def __do_analysis(self, analysis_id, min_cluster_size, user_email_add, is_public):
        print("ispublic%s"%is_public)
        mysql_acc.update_analysis_email_public(analysis_id, user_email_add, is_public)
        status, analysis_job_path= mysql_acc.get_status_and_file_path(analysis_id)
        print("analysis job file path: %s" % analysis_job_path)

        working_dir = os.path.dirname(os.path.normpath(analysis_job_path))

        print("working dir %s"%working_dir)

        try:
            if self.__is_analysis_started(status=status):
                print("The analysis job %06d has been started"%(analysis_id))
                return "The analysis job %06d has been started"%(analysis_id)
            accessionId = "E%06d"%(analysis_id)
            python_path = "/usr/bin/python3 "
            pipeline_path = "/home/ubuntu/mingze/tools/spectra-library-analysis/analysis_pipeline.py "
            parameter_accession_id = "-p " + accessionId + " "
            parameter_cluster_size = "-s %d " % min_cluster_size
            parameter_quiet = "-t y "
            command_line = python_path + pipeline_path + parameter_accession_id + parameter_cluster_size + parameter_quiet

        # this part is used to test_old if the invoke are working fine or not
        #     command_line = "/usr/bin/python3 /tmp/test_old.py"
        #     print("start to execute " + command_line)
        #     output = os.popen(command_line).readlines()
        #     print(''.join(output) + "\n")

        #rename the resultFile.txt fr running, avoid multiple jobs being invoked at the same time
            result_file =  os.path.join(analysis_job_path, "resultFiles.txt")
            running_result_file =  os.path.join(analysis_job_path, "resultFiles.txt.started")
            print("result file %s" %result_file)
            if os.path.isfile(result_file):
                os.rename(result_file, running_result_file)
            elif os.path.isfile(running_result_file):
                print("The resultFile.txt.started file exists, this job %s is in running status, cancel this analysis application."%(accessionId))
                return "The analysis job %s had been started before" % accessionId
            else:
                print("This job " + accessionId + " resultFile.txt is going wrong, please contact the administrator to have a check.")
                print("file:" + os.path.abspath(result_file))
                return "The analysis job " + accessionId + " is going wrong"

            command_line = python_path + pipeline_path + parameter_accession_id + parameter_cluster_size + parameter_quiet
            print("start to execute " + command_line)
            os.chdir(working_dir)
            output = os.popen(command_line).readlines()
            print(''.join(output) + "\n")
        except IOError as err:
            print(err)

        try:
            time.sleep(30)
        except Exception as err:
            print(err)

        status, analysis_job_path= mysql_acc.get_status_and_file_path(analysis_id)
        if self.__is_analysis_started(status):
            print("The analysis job is started")
            return "The analysis job is started"
        else :
            print("The analysis job fail to start, please contact the help group to have a check")
            return "The analysis job fail to start, please contact the help group to have a check"


    def post(self):

        analysis_id = int(request.headers.get('analysisId', None))
        min_cluster_size = int(request.headers.get('minClusterSize', None))
        user_email_add = request.headers.get('userEmailAdd', None)
        is_public = request.headers.get('isPublic', None)

        if not analysis_id or not min_cluster_size or not user_email_add or not is_public:
            return{'message':"missing parameters, please have a check", 'status':'error'}
        print("start to do analysis on project %d"%analysis_id)
        return_msg = self.__do_analysis(analysis_id, min_cluster_size, user_email_add, is_public)
        return return_msg, 201

class Test(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('para')

    def get(self):
        args = self.parser.parse_args()
        para = args['para']
        return_msg = {'status':"OK",
                      'message':"Got your input para: %s"%para
                      }
        return return_msg,200


##
## Actually setup the Api resource routing here
##
api.add_resource(DoAnalysis, '/analysis/do')
api.add_resource(FileUpload, '/file/upload')
api.add_resource(FileConfirm, '/file/confirm')
api.add_resource(Test, '/test')

if __name__ == '__main__':
    host = config.get("Web", "host")
    port = config.get("Web", "port")
    debug = config.get("Web", "debug")

    app.run(host=host, port=port, debug=debug)
    # DoAnalysis.do_analysis(DoAnalysis, analysis_id=54, min_cluster_size=10, user_email_add='bmze@qq.com', is_public=False)


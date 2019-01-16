from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
import os, sys, time
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)
import mysql_storage_access as mysql_acc

app = Flask(__name__)
api = Api(app)

def abort_if_todo_doesnt_exist(todo_id):
    if todo_id not in TODOS:
        abort(404, message="Todo {} doesn't exist".format(todo_id))


# File upload
#
class FileUpload(Resource):
    UPLOAD_FOLDER = '/data/phoenix_enhancer/test'
    ALLOWED_EXTENSIONS = set(['xml','mzid','mztab', 'mzML', 'mgf', 'MGF', 'gz'])
    parser = reqparse.RequestParser()
    parser.add_argument('jobId')
    parser.add_argument('accessionId')
    parser.add_argument('token')
    parser.add_argument('file', type=FileStorage, location='files', action='append')

    def allowed_file(self,filename):
        return '.' in filename and \
               filename.rsplit('.', 1)[1].lower() in self.ALLOWED_EXTENSIONS

    def post(self):
        data = self.parser.parse_args()
        uploaded_files = data['file']
        accession_id = data['accessionId']
        id = int(data['jobId'])
        token = data['token']

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
        if not os.path.exists(project_path):
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
                uploaded_file.save(os.path.join(self.UPLOAD_FOLDER,filename))
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


# Analysis Job apis
#
class FileConfirm(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('analysisId')
    parser.add_argument('resultFileList')


    def is_file_list_correct(self, result_file_list, file_dir):
        nonexist_files = list()
        for file in result_file_list.get('fileList'):
            filename = file.get('fileName')
            # filetype = file.get('fileType')
            if not os.path.isfile() or not os.path.getsize(os.path.join(file_dir, filename)):
                nonexist_files.append(filename)
        return nonexist_files

    def post(self):
        args = self.parser.parse_args()
        analysis_id = int(args['analysisId'])
        result_file_list = args['resultFileList']
        analysis_job = mysql_acc.get_analysis_job(analysis_id);
        nonexist_files = self.is_file_list_correct(result_file_list, analysis_job.get('file_path'))
        message = {'id':analysis_id, 'message':'', 'status':''}
        if len(nonexist_files):
            print("you got " + result_file_list.get('fileListLength') + ": " + str(result_file_list) + " files in AnalysisJob E%06d" % analysis_id);
            message['status'] = "success"
            status, file_dir = mysql_acc.get_status_and_file_path
            self.write_to_result_file(file_dir, result_file_list);
            mysql_acc.update_analysis_job_status(analysis_id, "uploaded");
        else:
            message['status'] = "error"
            message['message'] = "The file list is not as same as in the web server. " + \
                                 "These files don't exist or is empty: %s"%str(nonexist_files)
        return message



# Analysis Job apis
#
class DoAnalysis(Resource):

    parser = reqparse.RequestParser()
    parser.add_argument('analysisId')
    parser.add_argument('minClusterSize')
    parser.add_argument('userEmailAdd')
    parser.add_argument('isPublic')

    def __is_analysis_started(self, status):
        if (status.lower() == "started" or status.lower() == "finished" or status.lower() == "finished_with_error"):
            return True
        else:
            return False

    def __do_analysis(self, analysis_id, min_cluster_size, user_email_add, is_public):
        mysql_acc.update_analysis_email_public(analysis_id, user_email_add, is_public)
        status, analysis_job_file_path= mysql_acc.get_status_and_file_path(analysis_id)


        file_dir = os.path.dirname(analysis_job_file_path)
        working_dir = os.path.dirname(os.path.normpath(file_dir))

        print("working dir %s"%working_dir)

        try:
            if self.__is_analysis_started(status):
                print("The analysis job %06d has been started"%(analysis_id))
                return "The analysis job %06d has been started"%(analysis_id)
            accessionId = "E%06d"%(analysis_id)
            python_path = "/usr/bin/python3 "
            pipeline_path = "/home/ubuntu/mingze/tools/spectra-library-analysis/analysis_pipeline.py "
            parameter_accession_id = "-p " + accessionId + " "
            parameter_cluster_size = "-s %d " % min_cluster_size
            parameter_quiet = "-t y "
            command_line = python_path + pipeline_path + parameter_accession_id + parameter_cluster_size + parameter_quiet

        # this part is used to test if the invoke are working fine or not
        #     command_line = "/usr/bin/python3 /tmp/test.py"
        #     print("start to execute " + command_line)
        #     output = os.popen(command_line).readlines()
        #     print(''.join(output) + "\n")

        #rename the resultFile.txt fr running, avoid multiple jobs being invoked at the same time
            result_file =  os.path.join(file_dir, "resultFiles.txt")
            running_result_file =  os.path.join(file_dir, "resultFiles.txt.started")
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

        status, analysis_job_file_path= mysql_acc.get_status_and_file_path(analysis_id)
        if self.__is_analysis_started(status):
            print("The analysis job is started")
            return "The analysis job is started"
        else :
            print("The analysis job fail to start, please contact the help group to have a check")
            return "The analysis job fail to start, please contact the help group to have a check"


    def post(self):

        args = self.parser.parse_args()
        analysis_id = int(args['analysisId'])
        min_cluster_size = int(args['minClusterSize'])
        user_email_add = args['userEmailAdd']
        is_public = bool(args['isPublic'])
        return_msg = self.__do_analysis(analysis_id, min_cluster_size, user_email_add, is_public)
        return return_msg, 201


##
## Actually setup the Api resource routing here
##
api.add_resource(DoAnalysis, '/analysis/do')
api.add_resource(FileUpload, '/file/upload')
api.add_resource(FileConfirm, '/file/confirm')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port='5000', debug=True)


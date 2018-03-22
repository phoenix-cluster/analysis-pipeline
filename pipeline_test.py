from create_shell import CreateShell
from create_shell import FileName
import os
print("begin")

'''first step'''
test1=CreateShell()
choise = str(input("Will you use the script's default file path?：y/n\n"))
if choise=='y' or choise=="yes":
    a=test1.De_GunzipShell()
    a1=test1.De_GzipShell()
else:
    path=str(input("Please enter the absolute path of the folder you need to extract/compress: Such as\n/home/luoxiyang/test/\n"))
    b=test1.GunzipShell(path)
    b1=test1.GzipShell(path)
    
'''
choise=str(input("Do you want run the Shell?(y/n)\n"))
if choise=="y":
    os.system("bash gzip_test.sh")
else:
    os.sys.exit(0)
'''

'''second step'''
"""@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@change Path
"""
filepath="C:\\Users\\luo xi yang\\Desktop\\temp_workspace\\python"

getfilename=FileName(filepath)
getreturn = getfilename.get_file_name()
file_lenght=list(getreturn)[1]
all_name_list=list(getreturn)[0]
print(all_name_list)
'''create import_to_phoenix.sh'''
mulu = str(input("please input catalog:\n"))
import_to_phoenix=test1.create_import_to_phoenix_Shell(mulu,all_name_list)

'''run'''
'''
os.system("import_to_phoenix.sh")
'''

'''step 3'''
run_msconvert=test1.create_run_msconvert_Shell(mulu,all_name_list)

'''run'''
'''
os.system("run_msconvert.sh")
'''

'''step 4'''
run_spectrast=test1.create_run_spectrast_Shell(all_name_list)

'''run'''
'''
os.system("run_spectrast.sh")
'''

'''step 5'''
analyze=test1.create_analyze(mulu)

'''run'''
'''
os.system("analyze.sh")
'''






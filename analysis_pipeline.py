import os   
import re 
class CreateShell():
    def __init__(self):
        self.default_path="/home/luoxiyang/test/"
        self.default_begin="#!/bin/bash\n"
        self.default_do,self.default_done="do\n","done"
        self.default_zip,self.default_unzip,self.default_zipfile,self.default_unzipfile="gzip","gunzip","*.xml\n","*.gz\n"
        
    """De_GzipShell、De_GunzipShell用于执行当前文件夹中文件的压缩和解压
	GzipShell、GunzipShell需要输入需要压缩或解压的文件夹/文件的绝对路径
	该函数返回一个用于解压文件或压缩文件的shell脚本，可直接在linux中使用。
	"""   
    def De_GzipShell(self):
        b=[self.default_begin,"for i in *.xml\n",self.default_do,"gzip $i\n",self.default_done]
        with open("gzip_test_de.sh","w") as f1:
            for j in range(len(b)):
                f1.write(str(b[j]))
        print("ok")
        
    def GzipShell(self,path):
        temp1="for i in "
        a1=temp1+path+self.default_zipfile
        b1=[self.default_begin,a1,self.default_do,"gzip $i\n",self.default_done]
        with open("gzip_test.sh","w") as f1:
            for j in range(len(b1)):
                f1.write(str(b1[j]))
        print("ok")
        
    def De_GunzipShell(self):
        a=[self.default_begin,"for i in *.gz\n",self.default_do,"gunzip -d $i\n",self.default_done]
        
        with open("gunzip_test_de.sh","w") as f:
            for i in range(len(a)):
                f.write(str(a[i]))
        print("ok")
    
    def GunzipShell(self,path):
        temp1="for i in "
        a=temp1+path+self.default_unzipfile
        a1=[self.default_begin,a,self.default_do,"gunzip -d $i\n",self.default_done]
        
        with open("gunzip_test.sh","w") as f:
            for i in range(len(a1)):
                f.write(str(a1[i]))
        print("ok")
        
    def create_import_to_phoenix_Shell(self,mulu,filename):
        print(str((filename[0])[0]))
        print(mulu)
        t2=" -i "
        t3=".xml&\n"
        t4="_pasm.csv\n"
        t5="_spec.csv\n"
        xunhuan1 = "java -jar /home/ubuntu/mingze/tools/pridexml-to-phoenix/target/pridexml-to-phoenix-1.0-SNAPSHOT.jar -csv -m -ph -p "
        
        rm = "rm "
        rm1=rm+mulu+t4
        rm2=rm+mulu+t5
        
        content=[self.default_begin,rm1,rm2]
        with open("import_to_phoenix.sh","w") as f1:
            for j in range(len(content)):
                f1.write(str(content[j]))
        with open("import_to_phoenix.sh","a") as f2:
            for i in range(len(filename)):
                tempfilename=str((filename[i])[0])
                xunhuan=xunhuan1+mulu+t2+tempfilename+t3
                print(xunhuan)
                f2.write(str(xunhuan))
        print("ok")
        
    def create_run_msconvert_Shell(self,mulu,filename):
        t1=" time msconvert  /home/ubuntu/mingze/spec_lib_searching/phospho/"
        t2="/"
        t3=".mgf "
        t4="--mzML -o /home/ubuntu/mingze/spec_lib_searching/phospho/"
        t5="&\n"
        with open("run_msconvert.sh","w") as f:
            f.write(str(self.default_begin))
        with open("run_msconvert.sh","a") as f1:
            for i in range(len(filename)):
                tempfilename=str((filename[i])[0])
                xunhuan = t1+mulu+t2+tempfilename+t3+t4+mulu+t5
                print(xunhuan)
                f1.write(str(xunhuan))
        print("ok")
        
    def create_run_spectrast_Shell(self,filename):
        t1="time spectrast -sL /home/ubuntu/mingze/spec_lib_searching/201504-spec-lib-nofilter/201504_nofil_min5.splib "
        t2=".mzML&\n"
        
        with open("run_spectrast.sh","w") as f:
            f.write(str(self.default_begin))
        with open("run_spectrast.sh","a") as f1:
            for i in range(len(filename)):
                tempfilename=str((filename[i])[0])
                xunhuan = t1+tempfilename+t2
                print(xunhuan)
                f1.write(str(xunhuan))
        print("ok")
    
    def create_analyze(self,mulu):
        t1="/home/ubuntu/mingze/tools/spectra-library-analysis/enhancer_analyze.py -p "
        temp=t1+mulu
        with open("analyze.sh","w") as f:
            f.write(str(self.default_begin))
            f.write(str(temp))
        print("ok")
        
class FileName():
    def __init__(self,filepath):
        self.filepath=filepath
    def get_file_name(self):
        file_dir=self.filepath
        a=[]
        for root, dirs, files in os.walk(file_dir):
            #print(root) #当前目录路径
            #print(dirs) #当前路径下所有子目录
            print('s',str(files)) #当前路径下所有非目录子文件
            for i in range(len(files)):
                a.append(files[i])
        print('aa',str(a))
        b=[]
        for j in range(len(a)):
            print(a[j])
            rule=re.findall(r'(.*).xml',str(a[j]))
            if len(rule) !=0:
                b.append(rule)
        print(b)
        return b,len(b)
        
#a=file_name("C:\\Users\\luo xi yang\\Desktop\\temp_workspace\\python")
#a=file_name("/home/xiyangluo/test/")
#print(a)
        
        
        
        

        

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

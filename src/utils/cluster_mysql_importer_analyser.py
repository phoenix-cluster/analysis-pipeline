"""
This analyser import clusters from a .clustering file 
"""

import operator
# import phoenixdb
import os,sys
#sys.path.insert(0, "./py-venv/lib/python3.6/site-packages")
import re
import traceback
import pymysql.cursors
import json
import confident_score_calc as conf_sc_calc

package_path = os.path.abspath(os.path.split(sys.argv[0])[0]) + os.path.sep + ".."
sys.path.insert(0, package_path)

from spectra_cluster.analyser import common


class ClusterMySqlImporter(common.AbstractAnalyser):
    """
    This tool  import clustering files to MYSQL


    TODO: 
    """
    def __init__(self):
        """
        Initialised a new Cluster Importer Analyser.

        :return:
        """
        super().__init__()
        self.min_size = 2 # set default minium size 2
        self.type_to_import = ['a'] #import all types(cluster, spectrum, project) by default
        self.file_index = 0
        self.mysql_host = "20.20.10.181"
        self.mysql_port = 3306
        self.table_name = "T_CLUSTER".upper()
        self.table = None 
        self.over_write_table = False
        self.check_table_flag = True
        self.projects = set() 
       
        # intermediate data structures
        self.cluster_list = [] 

    def connect_and_check(self):
                #build the connection
        self.connection = pymysql.connect(host=self.mysql_host,
                                          port=self.mysql_port,
                                          user='phoenix_enhancer',
                                          password='enhancer123',
                                          db='phoenix_enhancer',
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        print("Opened database successfully");

        if self.check_table_flag:
            self.check_table()
 
    def check_table(self):
        """
        Check the table name exists or not,
        create it if not exists,
        ask user if they want to overwrite the table if exists.
        """
        table_exists = None
        create_new = None
        tb_exists = "SELECT COUNT(*) FROM " + self.table_name
        # create a table

        tb_create = "CREATE TABLE `" + self.table_name + "` ("                     + \
                        "id int(15) NOT NULL AUTO_INCREMENT,"    + \
                        "cluster_id varchar(100) COLLATE utf8_bin NOT NULL,"    + \
                        "cluster_ratio float,"    + \
                        "n_spec int(10),"    + \
                        "n_id int (10),"    + \
                        "n_unid int(10),"    + \
                        "sequences_ratios text ,"+ \
                        "sequences_mods text ,"+ \
                        "spectra_titles mediumtext NOT NULL ,"+  \
                        "consensus_mz text NOT NULL,"+   \
                        "consensus_intens text NOT NULL,"+ \
                        "conf_sc text NOT NULL,"+ \
                        "seq_taxids varchar(100),"+ \
                    "PRIMARY KEY (id)" + \
                        ")ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"

        with self.connection.cursor() as cursor:
            try:
                print(tb_exists)
                cursor.execute(tb_exists)
                result = cursor.fetchone()
                if result != None :
                    table_exists = True
       #except IOError as e:
                else:
                    print("Table does not exists")
                    table_exists = False
                    create_new = True
            except Exception as e:
                if str(e).startswith("(1146, \"Table") and str(e).endswith("doesn't exist\")"):
                    print(e)
                    table_exists = False
                    create_new = True
                else:
                    raise e
            try:
                if table_exists and not self.over_write_table:
                    print("1" + str(table_exists))
                    print("2" + str(self.over_write_table))
                    print("The table" + str(self.mysql_host) + ":" + str(self.mysql_port) + " - " + self.table_name + "is already exists, do you really want to overwrite it?")
                    answer = input("please input yes | no:  ")
                    while(answer != 'yes' and answer != 'no'):
                        answer = input("please input yes | no:")
                    if answer == 'no':
                        print("Going to exit.")
                        sys.exit(0)
                    else:
                        create_new = True

                if self.over_write_table or create_new :
                    if table_exists:
                        print("Start droping the tables")
                        # cursor.execute("DROP TABLE IF EXISTS \"" + self.table_name + "_spec\"")
                        # cursor.execute("DROP TABLE IF EXISTS \"" + self.table_name + "_projects\"")
                        cursor.execute("DROP TABLE IF EXISTS " + self.table_name)
                    print("Start creating table " + self.table_name)
                    print(tb_create)
                    cursor.execute(tb_create)
                    # print(tb_create_prjs)
                    # cursor.execute(tb_create_prjs)
                    # print(tb_create_spec)
                    # cursor.execute(tb_create_spec)
            except Exception as e:
                print(e)
            finally:
                print ("checked table")

    def process_cluster(self, cluster):
        """
        Add the clusters into cluster list,
	    the projectID, assay file name and spectrum index has been extracted from the spectrum title

        :param cluster: the cluster to process
        :return:
        """
        if self._ignore_cluster(cluster):
            return

        self.cluster_list.append(cluster)

    # Only support PXD000000 or PRD000000 style project id here, need to modify here
    # to support other project id which is in the spectrum title
    def get_project_id(self, title):
        matchObj = re.match( r'.*?(P[XR]D\d{6}).*', title)
        if matchObj:
#            print("got match" + matchObj.group(1))
            return matchObj.group(1)
        else:
            print("No PRD000000 or PXD000000 be found in the title" + title)
            return None

    #todo: do we need to replace I for L? not yet
    def get_seqs_mods(self, spectrum, seq_mods_map):
        sequences = list()
        all_mods = list() 
        for psm in spectrum.psms:
            clean_seq = re.sub(r"[^A-Z]", "", psm.sequence.upper());
            if clean_seq not in sequences: 
                sequences.append(clean_seq)
                ptms = list()
                for ptm in psm.ptms:
                    ptms.append(str(ptm))
                ptm_str = ",".join(ptms)
                all_mods.append(ptm_str)
                if clean_seq not in seq_mods_map.keys():
                    seq_mods_map[clean_seq] = ptm_str
                
        sequences_str = "||".join(sequences)
        all_mods_str = ";".join(all_mods)
        return(sequences_str, all_mods_str, seq_mods_map)


    def import_projects(self):
        if 'a' in self.type_to_import or 'p' in self.type_to_import:
            try:
                with self.connection.cursor() as cursor:
                    for project_id in self.projects:
                        INSERT_sql = "INSERT INTO `" + self.table_name + "_projects`" \
                                     "(project_id)" + \
                                     "VALUES" + \
                                     "('" + project_id + "')"
                        cursor.execute(INSERT_sql)
            except Exception as e:
                print(e)


    def get_seq_taxids(self, cluster_ratio, cluster_seqs_ratios, seq_taxid_map):
        cluster_seq_ratio_map = json.loads(cluster_seqs_ratios.replace("'", "\""))
        major_seq = ""
        for seq, ratio in cluster_seq_ratio_map.items():
            if cluster_ratio == ratio:
                major_seq = seq
                break
        taxids = str(seq_taxid_map.get(major_seq)).replace("'","")
        return taxids


    def import_afile(self):
        """
        import the cluster list  of a file to phoenix

        self.id = cluster_id
        self.precursor_mz = precursor_mz
        self.consensus_mz = consensus_mz
        self.consensus_intens = consensus_intens
        self.n_spectra = len(self._spectra)
        self.identified_spectra = 0
        self.unidentified_spectra = 0

        spectrum:
        self.title = title
        self.precursor_mz = precursor_mz
        self.charge = charge
        self.taxids = frozenset(taxids)
        project_id
        
                        "spectra_titles VARCHAR "    + \
                        "consensus_mz VARCHAR "    + \
                        "consensus_intens VARCHAR "    + \
        
        """
        try:
            with self.connection.cursor() as cursor:
                INSERT_sql = "INSERT INTO `" + self.table_name + "`" \
                            "(cluster_id, cluster_ratio, n_spec, n_id, n_unid," + \
                            " sequences_ratios, sequences_mods, spectra_titles, consensus_mz, consensus_intens," + \
                            "conf_sc, seq_taxids)" + \
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cluster_data = []
                spec_data = []
                for cluster in self.cluster_list:
                    spectra = cluster.get_spectra()
                    sequences_ratios = str(cluster.get_sequence_ratios_il())
                    sequence_taxids_map =  cluster.sequence_taxids_map
                    seq_mods_map = {}
                    n_spec = cluster.n_spectra or 0
                    n_id = cluster.identified_spectra or 0
                    n_unid = cluster.unidentified_spectra or 0
                    spectra_titles = ""
                    consensus_mz = ",".join(map(str,cluster.consensus_mz))
                    consensus_intens = ",".join(map(str,cluster.consensus_intens))
                    seq_taxids = self.get_seq_taxids(float(cluster.max_il_ratio), sequences_ratios, sequence_taxids_map)

                    cluster_dict = {'id':cluster.id, 'ratio':float(cluster.max_il_ratio), 'size':n_spec, 'seqs_ratios':sequences_ratios}
                    scores = str(conf_sc_calc.calculate_conf_sc_for_a_cluster(cluster_dict))
                    print(scores)

#                    INSERT_data = "('" + cluster.id + "', " + str(cluster.max_il_ratio) + ", " + str(cluster.n_spectra) + ", " + str(cluster.identified_spectra) + \
#                            ", " + str(cluster.unidentified_spectra) + ", '" + max_sequences + "'),"
                    for spectrum in spectra:
                        if spectra_titles != "":
                            spectra_titles = spectrum.title + "||" + spectra_titles 
                        else:
                            spectra_titles = spectrum.title 
                        project_id = self.get_project_id(spectrum.title)
                        taxids = ",".join(spectrum.taxids)
                        #get the ratio of this spectra/sequence
                        max_seq_ratio = 0
                        for seq in spectrum.get_clean_sequences():
                            seq = seq.replace("I", "L")
                            if cluster.sequence_ratios_il[seq] > max_seq_ratio:
                                max_seq_ratio = cluster.sequence_ratios_il[seq]

                        (sequences, modifications, seq_mods_map) = self.get_seqs_mods(spectrum, seq_mods_map)
                        self.projects.add(project_id)
#                        INSERT_sql2 +=   "('" + spectrum.title + "', '" + project_id + "', " + str(int(spectrum.is_identified())) + ", '" + cluster.id +"'),"
#                         INSERT_data2 =   (spectrum.title , project_id , spectrum.charge, spectrum.precursor_mz, taxids, spectrum.is_identified(), sequences, modifications, max_seq_ratio, cluster.id )
                        if spectrum.title == None or len(spectrum.title)<1:
                            print("Wrong spectrum title: " + spectrum.title)
                        # spec_data.append(INSERT_data2)
                    seq_mods_str = str(seq_mods_map)
                    INSERT_data = (cluster.id, float(cluster.max_il_ratio), n_spec, n_id, + \
                        n_unid, sequences_ratios, seq_mods_str, spectra_titles, consensus_mz, consensus_intens, scores, seq_taxids)
                    cluster_data.append(INSERT_data)
#                print(cluster_data)
                if 'a' in self.type_to_import or 'c' in self.type_to_import:
                    print("start to import clusters in a file")
                    try:
                        cursor.executemany(INSERT_sql, cluster_data)
                        self.connection.commit()
                    except Exception as ex:
                        print(ex)
                # if 'a' in self.type_to_import or 's' in self.type_to_import:
                #     print("start to import spectra in a file")
                #     cursor.executemany(INSERT_sql2, spec_data)
                print(str(len(cluster_data)) + "clusters have been imported in file.")
        except Exception as ex:
            print(ex)
            traceback.print_exc(file=sys.stdout)
        finally:
            print("INSERTed a cluster list in to table")

    def clear(self):
        """
        clear the cluster list for a new file
        """
        self.cluster_list = [] 
    def close_db(self):
        self.connection.close()
    


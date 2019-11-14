import pandas as  pd
import numpy as np
import glob as glob
import sys

"""
This program remove the spec archive searchi results, which are map to the clusters only have spectra from same projects.
spec1_in_project1 --match-- cluster1_which_only_has_spec_from_project1, remove this match
"""

prj_dirs= glob.glob('PXD*')
clusters = pd.read_csv('201504_min5_clustering_with_prj.tab', sep='\t', header=0)
clusters = clusters.set_index('cluster_id')
for prj in prj_dirs:
    n_removed = 0
    search_result = pd.read_csv(prj + '/lib_search_result.tab', sep='\t', header=0)
    output_file = prj + '/lib_search_result.fil.tab'
    with open (output_file,'w') as o:
        line = "%s\t%s\t%s\t%s\n"% \
               ("spec_title", "spec_in_lib", "dot", "fval")
        o.write(line)
        for i in range(0,len(search_result)):
    #    for i in range(0,10):
            match = search_result.iloc[i]
            cluster_id = match['spec_in_lib']
            cluster = clusters.loc[cluster_id]
            projects = cluster['projects'].split('||')

            if len(projects) == 1 and projects[0] == prj:
                n_removed += 1
                continue
            line = "%s\t%s\t%s\t%s\n"% \
                   (match["spec_title"], cluster_id, match["dot"], match["fval"])
            o.write(line)
        o.write("Removed %d (%.1f%%) query-project-only clusters for project %s\n"%(n_removed, n_removed/len(search_result), prj)) 

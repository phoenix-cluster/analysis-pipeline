# from docopt import docopt
# import sys
# import csv
import os,sys
#sys.path.insert(0, "../py-venv/lib/python3.6/site-packages")
import operator
# import ntpath
# import lxml
# etree = lxml.html.etree
# print(lxml)
from lxml import etree

from pathlib import Path
# try:
#     import xml.etree.cElementTree as ET
# except ImportError:
#     import xml.etree.ElementTree as ET
import re
from requests import structures as structures

def get_scfield_peakfile(filename):
    """
    get score field name from the mzident xml file
    get source peak file name from the mzident xml file
    :param filename:
    :return:
    """
    #get score field
    score_fields=[
        "Scaffold:Peptide Probability",
        "MS-GF:SpecEValue",
        "X\\!Tandem:expect",
        "mascot:expectation value"
    ]

    tree = etree.parse(filename)

    root = tree.getroot()
    namespace = get_namespace(root)

    xpath_tag_str = ".//*[translate(local-name(), \"ABCDEFGHIJKLMNOPQRSTUVWXYZ\", \"abcdefghijklmnopqrstuvwxyz\")=$tagname]"
    ident_elem_list = root.xpath(xpath_tag_str, tagname="spectrumidentificationitem", namespaces={'ns':namespace[1:-1]})
    print("len of id elem list %d"%len(ident_elem_list))
    spec_ident_all_attrib = ""
    score_field = ""
    for id_elem in ident_elem_list:
        for subelem in list(id_elem):
            spec_ident_all_attrib += str(subelem.attrib).lower()
        for temp_field in score_fields:
            if temp_field.lower() in spec_ident_all_attrib:
                score_field = temp_field
                break
        if score_field == "":
            raise Exception("Failed to find score field in mzIdentML file.")
        else:
            print("Find score field %s in meIdentML file %s"%(score_field, filename))
            break
    print("start to get peak files")
    #get peak files
    peak_files = list()
    spectra_list = root.xpath(xpath_tag_str, tagname="spectradata", namespaces={'ns':namespace[1:-1]})
    for spec_data in spectra_list:
        location = spec_data.attrib['location']
        norm_peak_file_path =  os.path.normpath(location)
        norm_peak_file_path = norm_peak_file_path.replace('\\', os.sep)
        peak_file_name = os.path.basename(norm_peak_file_path)
        peak_files.append(peak_file_name)
    if len(peak_files)> 1:
        raise Exception("MzIdentML file %s has multiple peak files: %s, %s..."%(filename, peak_files[0], peak_files[1]))

    return (score_field, peak_files[0])

########################
    """
    tree = ET.ElementTree(file=filename)
    root = tree.getroot()
    namespace = get_namespace(tree.getroot())


    attrib_string = ""
    for elem in tree.iter(tag="%sSpectrumIdentificationItem" % (namespace)):
        for subelem in list(elem):
            attrib_string += str(subelem.attrib)
            for temp_field in score_fields:
                if temp_field in str(subelem.attrib):
                    score_field = temp_field
                    break

        break #only check one SpectrumIdentificationItem
    if not score_field:
        raise Exception("Failed to find supplied score field '" +
                        "' in mzIdentML file %s. \nDetails:\n%s"%(filename, str(attrib_string)))

    #get peak files
    peak_files = list()
    for spec_data in tree.iter(tag="%sSpectraData" % (namespace)):
        location = spec_data.attrib['location']
        peak_file_name = ntpath.basename(location)
        peak_files.append(peak_file_name)
    """


def get_score_from_spec_ident(spec_ident, score_field):
    for subelem in list(spec_ident):
        if score_field.lower() in str(subelem.attrib).lower():
            return structures.CaseInsensitiveDict(subelem.attrib).get('value')
    return None

def get_scan_num_from_xtanem_spec_ref(spec_ref):
    for subelem in list(spec_ref):
        if "scan number(s)" in str(subelem.attrib).lower():
            return structures.CaseInsensitiveDict(subelem.attrib).get('value')
    return None

def get_para_value_from_subelem(elem, paraname):
    for subelem in list(elem):
        if paraname.lower() == subelem.tag.lower():
            return subelem.text
    raise Exception("Fail to get para value %s from elem %s"%(paraname, elem.attrib))

def get_sub_attrib(elem):
    sub_elem_attribs = ""
    for subelem in list(elem):
        sub_elem_attribs += str(subelem.attrib).lower()
    return sub_elem_attribs

def get_modification_acc_from_mass_delta(mass_delta):
    return "todo"


def get_modification_acc(mod_elem):
    mass_delta = structures.CaseInsensitiveDict(mod_elem.attrib).get("monoisotopicMassDelta")
    for subelem in list(mod_elem):
        attribs = structures.CaseInsensitiveDict(subelem.attrib)
        if attribs.get("cvRef")=="UNIMOD" or attribs.get("cvRef")=="MOD":
            return attribs.get("accession")
        else:
            return get_modification_acc_from_mass_delta(mass_delta)
    raise Exception("Fail to get modification accession from %s, %s"%(str(mod_elem.tag), str(mod_elem.attrib)))

def get_namespace(element):
    m = re.match('\{.*\}', element.tag)
    return m.group(0) if m else ''


def parser_mzident(filename, score_field, title_field=None,
                   fdr=0.01, larger_score_is_better=False, decoy_string="DECOY",
                   include_decoy=False):
    """
    A general parsing function for mzIdentML files.

    Several exporters of mzIdentML do not report the correct spectrum indexes. X!Tandem, for example,
    uses the spectrum's title as "id" instead of the correct "index=N" format for MGF files. Therefore,
    it is possible to supply the index_field and title_field separately. Later, missing indexes will be
    resolved through the titles.

    :param filename: The path to the mzIdentML file
    :param score_field: The name of the score's field (**Important**: do not supply the accession
                        but only the name)
    :param title_field: The name of the field supplying the spectrum's title (in SpectrumIdentificationResult).
    :param fdr: Target FDR (default 0.01). If set to "2" the original cut-off is used.
    :param larger_score_is_better: Logical indicating whether better scores mean a more reliable
                                   result. Default is False as most search engines report
                                   probabilities
    :param decoy_string: String used to identify decoy proteins.
    :param include_decoy: If set to True decoy hits are also returned.
    :return: A list of PSM objects
    mzid_psms = list()

    # load all PSMs from the file
    # with mzid.read(filename, use_index=False) as object_reader:

    tree = ET.ElementTree(file=filename)
    root = tree.getroot()
    namespace = get_namespace(tree.getroot())

    peptide_ref_ids = dict()
    for peptide_elem in root.iter("%sPeptide"%namespace):
        peptide = dict()
        peptide_ref_id = peptide_elem.get('id')
        peptide["seq"] = get_para_value_from_subelem(peptide_elem, namespace + "PeptideSequence")
        mods = ""
        for subelem in peptide_elem:
            if subelem.tag == "%sModification"%namespace:
                mods = "" + subelem.get("location") + "-" + get_modification_acc(subelem) + ","
        if len(mods) > 1:
            mods = mods[:-1] #remove the last ","
        peptide["mods"] = mods
        peptide_ref_ids[peptide_ref_id] = peptide

    decoy_peps = list()
    for pep_evid_elem in root.iter("%sPeptideEvidence"%namespace):
        is_decoy = False
        if pep_evid_elem.get("isDecoy") == "true":
            is_decoy = True
        elif "accession" in pep_evid_elem.attrib.keys():
            is_decoy = is_decoy or decoy_string in peptide_evidence.get("accession")
        elif "protein description" in pep_evid_elem.attrib.keys():
            is_decoy = is_decoy or decoy_string in peptide_evidence.get("protein description")

        if is_decoy:
            peptide_ref_id = pep_evid_elem.get("peptide_ref")
            decoy_peps.append(peptide_ref_id)

    for spec_ref in root.iter("%sSpectrumIdentificationResult"%namespace):
        for spec_ident in spec_ref.iter("%sSpectrumIdentificationItem"%namespace):
            # filter based on original FDR if set right away
            if fdr == 2 and not spec_ident.get("passThreshold"):
                continue
            # only use rank 1 ids
            if int(spec_ident.get("rank")) > 1:
                continue

            spec_ident_all_attrib = ""
            for subelem in list(spec_ident):
                spec_ident_all_attrib += str(subelem.attrib)

            if score_field not in spec_ident_all_attrib:
                raise Exception("Failed to find supplied score field '" + score_field +
                                "' in mzIdentML file.")

            if title_field is not None and title_field not in str(spec_ref.attrib):
                raise Exception("Failed to find supplied title field '" + title_field +
                                "' in mzIdentML file.")
            mzid_psm = dict()

            score = get_score_from_spec_ident(spec_ident, score_field)
            if not score:
                raise Exception("Failed to find supplied score from spec_ident" )
            mzid_psm["score"] = score

            # the index should be used as id
            if spec_ref.get("spectrumID")[:6] == "index=":
                mzid_psm["index"] = int(spec_ref.get("spectrumID")[6:])
            elif "scan number(s)" in get_sub_attrib(spec_ref):
                # TODO: This has only been tested for X!Tandem
                mzid_psm["index"] = int(get_scan_num_from_xtanem_spec_ref(spec_ref)) - 1
            else:
                mzid_psm["index"] = Psm.MISSING_INDEX

            # spectrum title is optional in mzIdentML
            if title_field is not None:
                mzid_psm["title"] = spec_ref.get(title_field).strip()
            elif "spectrum title" in str(spec_ref.attrib):
                mzid_psm["title"] = spec_ref.get("spectrum title").strip()

            if "peptide_ref" not in str(spec_ident.attrib):
                raise Exception("Error, can not found peptide_ref in %s"%(spec_ident.attrib))
            peptide_ref_id = spec_ident.get("peptide_ref")
            peptide = peptide_ref_ids.get(peptide_ref_id)
            mzid_psm["sequence"] = peptide.get("seq")

            if "Modification" in spec_ident:
                mzid_psm["ptms"] = convert_mzid_modifications(spec_ident["Modification"])
            elif peptide.get("mods"):
                mzid_psm["ptms"] = peptide.get("mods")

            is_decoy = False
            if peptide_ref_id in decoy_peps:
                is_decoy = True

            mzid_psm["is_decoy"] = is_decoy

            mzid_psms.append(mzid_psm)

    # sort the psms based on probability
    mzid_psms.sort(key=operator.itemgetter('score'), reverse=larger_score_is_better)

    # filter decoys
    filtered_psms = list()
    n_target = 0
    n_decoy = 0

    for mzid_psm in mzid_psms:
        # only filter if the FDR wasn't set to 2
        if fdr != 2:
            if mzid_psm["is_decoy"]:
                n_decoy += 1
            else:
                n_target += 1

            current_fdr = n_decoy * 2 / (n_target + n_decoy)

            if current_fdr > fdr:
                break

        if not mzid_psm["is_decoy"] or include_decoy:
            filtered_psms.append(mzid_psm)
            # filtered_psms.append(Psm(mzid_psm["index"], mzid_psm["sequence"], mzid_psm.get("title",None),
            #                          is_decoy=mzid_psm["is_decoy"], ptms=mzid_psm.get("ptms", None)))

    return filtered_psms
    """

def parser_mzident2(filename, score_field, title_field=None,
                   fdr=0.01, larger_score_is_better=False, decoy_string="DECOY",
                   include_decoy=False):
    """
    A general parsing function for mzIdentML files.

    Several exporters of mzIdentML do not report the correct spectrum indexes. X!Tandem, for example,
    uses the spectrum's title as "id" instead of the correct "index=N" format for MGF files. Therefore,
    it is possible to supply the index_field and title_field separately. Later, missing indexes will be
    resolved through the titles.

    :param filename: The path to the mzIdentML file
    :param score_field: The name of the score's field (**Important**: do not supply the accession
                        but only the name)
    :param title_field: The name of the field supplying the spectrum's title (in SpectrumIdentificationResult).
    :param fdr: Target FDR (default 0.01). If set to "2" the original cut-off is used.
    :param larger_score_is_better: Logical indicating whether better scores mean a more reliable
                                   result. Default is False as most search engines report
                                   probabilities
    :param decoy_string: String used to identify decoy proteins.
    :param include_decoy: If set to True decoy hits are also returned.
    :return: A list of PSM objects
    """
    mzid_psms = list()

    # load all PSMs from the file
    # with mzid.read(filename, use_index=False) as object_reader:

    tree = etree.parse(filename)

    root = tree.getroot()
    namespace = get_namespace(root)

    xpath_tag_str = ".//*[translate(local-name(), \"ABCDEFGHIJKLMNOPQRSTUVWXYZ\", \"abcdefghijklmnopqrstuvwxyz\")=$tagname]"
    peptide_elem_list = root.xpath(xpath_tag_str, tagname="peptide", namespaces={'ns':namespace[1:-1]})
    print("len of pep elem list %d"%len(peptide_elem_list))

    peptide_ref_ids = dict()
    for peptide_elem in peptide_elem_list:
        peptide = dict()
        peptide_ref_id = structures.CaseInsensitiveDict(peptide_elem.attrib).get('id')
        peptide["seq"] = get_para_value_from_subelem(peptide_elem, namespace + "PeptideSequence")
        mods = ""
        for subelem in peptide_elem:
            if subelem.tag.lower() == "%smodification"%namespace.lower():
                mods = "" + structures.CaseInsensitiveDict(subelem.attrib).get("location") + "-" + get_modification_acc(subelem) + ","
        if len(mods) > 1:
            mods = mods[:-1] #remove the last ","
        peptide["mods"] = mods
        peptide_ref_ids[peptide_ref_id] = peptide

    print("got %d peptide ref identifications" % len(peptide_ref_ids))
    decoy_peps = list()
    for pep_evid_elem in root.xpath(xpath_tag_str, tagname="peptideevidence", namespaces={'ns':namespace[1:-1]}):
        is_decoy = False
        evid_attribs = structures.CaseInsensitiveDict(pep_evid_elem.attrib)
        if evid_attribs.get("isDecoy") == "true" or evid_attribs.get("isDecoy".lower()) == "true":
            is_decoy = True
        elif "accession" in evid_attribs.keys():
            is_decoy = is_decoy or decoy_string in evid_attribs.get("accession")
        elif "protein description" in evid_attribs.keys():
            is_decoy = is_decoy or decoy_string in evid_attribs.get("protein description")

        if is_decoy:
            peptide_ref_id = evid_attribs.get("peptide_ref")
            decoy_peps.append(peptide_ref_id)
    print("got %d decoy identifications" % len(decoy_peps))

    analysis_data = root.xpath(xpath_tag_str, tagname="analysisdata", namespaces={'ns':namespace[1:-1]})[0]
    ident_list = analysis_data.xpath(xpath_tag_str, tagname="spectrumidentificationlist", namespaces={'ns':namespace[1:-1]})[0]

    for spec_ref in ident_list.xpath(xpath_tag_str, tagname="spectrumidentificationresult", namespaces={'ns':namespace[1:-1]}):
        spec_ref_attribs = structures.CaseInsensitiveDict(spec_ref.attrib)
        for spec_ident in spec_ref.xpath(xpath_tag_str, tagname="spectrumidentificationitem", namespaces={'ns':namespace[1:-1]}):
            spec_ident_attribs = structures.CaseInsensitiveDict(spec_ident.attrib)
            # filter based on original FDR if set right away
            if fdr == 2 and not spec_ident.get("passThreshold"):
                continue
            # only use rank 1 ids
            if int(spec_ident.get("rank")) > 1:
                continue

            spec_ident_all_attrib = ""
            for subelem in list(spec_ident):
                spec_ident_all_attrib += str(subelem.attrib).lower()
            print(score_field)
            if score_field.lower() not in spec_ident_all_attrib:
                raise Exception("Failed to find supplied score field '" + score_field +
                                "' in mzIdentML file.")

            if title_field is not None and title_field.lower not in str(spec_ref.attrib).lower():
                raise Exception("Failed to find supplied title field '" + title_field +
                                "' in mzIdentML file.")
            mzid_psm = dict()

            score = get_score_from_spec_ident(spec_ident, score_field)
            if not score:
                raise Exception("Failed to find supplied score from spec_ident" )
            mzid_psm["score"] = score

            # the index should be used as id
            if spec_ref_attribs.get("spectrumID")[:6] == "index=":
                mzid_psm["index"] = int(spec_ref_attribs.get("spectrumID")[6:])
            elif "scan number(s)" in get_sub_attrib(spec_ref):
                # TODO: This has only been tested for X!Tandem
                mzid_psm["index"] = int(get_scan_num_from_xtanem_spec_ref(spec_ref)) - 1
            else:
                mzid_psm["index"] = Psm.MISSING_INDEX

            mzid_psm['charge'] = spec_ident_attribs.get('chargeState', 'NA') #for missing charge in peak file
            mzid_psm['prec_mz'] = spec_ident_attribs.get('experimentalMassToCharge', 'NA')# for double check is correct spec

            # spectrum title is optional in mzIdentML
            if title_field is not None:
                mzid_psm["title"] = spec_ref_attribs.get(title_field).strip()
            elif "spectrum title" in str(spec_ref.attrib).lower():
                mzid_psm["title"] = spec_ref_attribs.get("spectrum title").strip()

            if "peptide_ref" not in str(spec_ident.attrib).lower():
                raise Exception("Error, can not found peptide_ref in %s"%(spec_ident.attrib))

            peptide_ref_id = spec_ident_attribs.get("peptide_ref")
            peptide = peptide_ref_ids.get(peptide_ref_id)
            mzid_psm["sequence"] = peptide.get("seq")
            # if "Modification" in spec_ident:
            #     mzid_psm["ptms"] = convert_mzid_modifications(spec_ident["Modification"])
            if peptide.get("mods"):
                mzid_psm["ptms"] = peptide.get("mods")

            is_decoy = False
            if peptide_ref_id in decoy_peps:
                is_decoy = True

            mzid_psm["is_decoy"] = is_decoy

            mzid_psms.append(mzid_psm)

    # sort the psms based on probability
    mzid_psms.sort(key=operator.itemgetter('score'), reverse=larger_score_is_better)

    # filter decoys
    filtered_psms = list()
    n_target = 0
    n_decoy = 0

    for mzid_psm in mzid_psms:
        # only filter if the FDR wasn't set to 2
        if fdr != 2:
            if mzid_psm["is_decoy"]:
                n_decoy += 1
            else:
                n_target += 1

            current_fdr = n_decoy * 2 / (n_target + n_decoy)

            if current_fdr > fdr:
                break

        if not mzid_psm["is_decoy"] or include_decoy:
            filtered_psms.append(mzid_psm)
            # filtered_psms.append(Psm(mzid_psm["index"], mzid_psm["sequence"], mzid_psm.get("title",None),
            #                          is_decoy=mzid_psm["is_decoy"], ptms=mzid_psm.get("ptms", None)))

    return filtered_psms



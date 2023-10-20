"""
This function processes all XML laws in the folder ./de_federal_raw
and writes them to ./de_federal_json as individual JSON files.
Finally, it merges all JSON files to one ./de_federal.json file.
This script using multiprocessing using the available CPUs of your machine.

1) Create a virtual environment:
python3 -m venv ./.venv
source ./.venv/bin/activate

2) Install dependencies:
pip3 install bs4 lxml tiktoken tqdm

3) Run this script:
python3 process_de_laws.py
"""

import os
import json
from datetime import datetime
from bs4 import BeautifulSoup
from typing import Dict, Union, Optional
import re
import tiktoken
import copy
import multiprocessing
from tqdm import tqdm

# Constants
OUTPUT_FILENAME = 'de_federal'  # .json
XML_DIR_PATH = "./de_federal_raw"  # Folder must exist
JSON_DIR_PATH = "./de_federal_json"  # Folder must exist
FILE_FILTER = ('')  # ('BJNR002190897', 'BJNR119530979')
XML_FILENAMES = [f for f in os.listdir(XML_DIR_PATH) if f.endswith(FILE_FILTER+('.xml'))]

# Initialize output dict
all_laws = {}
file_keys = {}  # this dictionary will keep track of the files processed under each key


def convert_xml_to_dict(element, expected_type: Optional[type] = None) -> Union[str, Dict]:
    """
    Function to recursively convert xml element and its children into dictionary.
    """
    if element.string:
        return element.string
    else:
        children_dict = {}
        for child in element.contents:
            if child.name:
                if child.name in children_dict:
                    if isinstance(children_dict[child.name], list):
                        children_dict[child.name].append(convert_xml_to_dict(child))
                    else:
                        children_dict[child.name] = [children_dict[child.name], convert_xml_to_dict(child)]
                else:
                    children_dict[child.name] = convert_xml_to_dict(child)
        # The final return should be a dict (when this function is not called by itself)
        if expected_type is not None and not isinstance(children_dict, expected_type):
            raise ValueError(f"Expected {expected_type} but got {type(children_dict)}")
        return children_dict


def num_tokens_from_string(string: str) -> int:
    """
    Function to count the number of tokens in a string
    https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
    """
    encoding = tiktoken.get_encoding('cl100k_base')
    num_tokens = len(encoding.encode(string))
    return num_tokens


def process_file(filename):
    """
    For each XML file
    """
    # Read file
    file_path = os.path.join(XML_DIR_PATH, filename)
    with open(file_path, encoding="utf8") as file:
        file_content = file.read()

        # Init this to store unprocessed Absätze.
        unprocessed_absatze = []

        # Parse XML with BeautifulSoup
        soup = BeautifulSoup(file_content, "lxml-xml")

        """
        Law Metadata
        """
        output = {
            'meta': {
                'source': filename,
                'download_date': datetime.fromtimestamp(os.path.getctime(file_path)).strftime("%Y-%m-%d"),
                'title': '',
                'last_changed': '',
                'alt_title': '',
            },
            'metadaten': convert_xml_to_dict(soup.metadaten, dict),
            'norms': []
        }
        output['meta']['last_changed'] = output['metadaten'].get('ausfertigung-datum')
        try:
            output['meta']['title'] = soup.metadaten.langue.text
        except AttributeError:
            pass

        """
        Get the unique key (such as 'BGB') of each law/gesetz. We are jusing jurabk, but if it is not available, we use amtabk.
        It is not fully clear what these abbreviations mean, but likely:
        jurabk = Judicial abbreviation of the law.
        amtabk = Official (Amtliche) abbreviation of the law.
        We prefere jurabk over amtabk because it seems to be more common.
        """
        key_planned = output['metadaten'].get('jurabk', output['metadaten'].get('amtabk'))
        # In rare cases, a law has multiple of these keys. In that case, we will use the first one.
        while isinstance(key_planned, list):
            key_planned = key_planned[0]

        def remove_year_from_key(key_planned):
            if isinstance(key_planned[-4:], str) and key_planned[-4:].isdigit():
                key_planned = key_planned[:-4].strip()
            return key_planned

        """
        We have some edge cases, where neither jurabk nor amtabk is a good name for the law. For example, UStG is called UStG 1980.
        So, if the key ends with a year, remove the year unless that would cause duplicates. There are laws where the year at the end makes sense.
        """
        # If the key ends with 4 digits, it ends with a year.
        key_process = remove_year_from_key(key_planned)
        # If the key is not unique
        if key_process in file_keys or key_planned in file_keys:
            previous_output_key = key_process
            # If a duplicate is found, rename the previous law to a law with the year suffix.
            # Get the key we want to use for the previous law instead (with the year)
            corrected_previous_output_key = all_laws[previous_output_key]['metadaten'].get(
                'jurabk', all_laws[previous_output_key]['metadaten'].get('amtabk')
              )
            # Rename the previous law's key by writing it again and deleting the old entry.
            if (corrected_previous_output_key != previous_output_key):
                all_laws[corrected_previous_output_key] = all_laws[previous_output_key]
                del all_laws[previous_output_key]
                # Do the same for file_keys
                file_keys[corrected_previous_output_key] = file_keys[previous_output_key]
                del file_keys[previous_output_key]
            # Since stripping the year causes duplicates, we will use the key with the year for this law too.
            key_process = key_planned
        file_keys[key_process] = filename

        alt_jurabk = output['metadaten'].get('jurabk')
        if (alt_jurabk):
            alt_jurabk = remove_year_from_key(alt_jurabk)
        alt_amtabk = output['metadaten'].get('amtabk')
        if (alt_amtabk):
            alt_amtabk = remove_year_from_key(alt_amtabk)

        # If both myjurabk and myamtabk are not None and not identical, print
        if alt_jurabk and alt_amtabk and alt_jurabk != alt_amtabk:
            # Save the one that is not key_planned to alt_title
            if alt_jurabk != key_planned:
                output['meta']['alt_title'] = str(alt_jurabk)
            else:
                output['meta']['alt_title'] = str(alt_amtabk)

        """
        Get the norms of the law
        """
        for law in soup.find_all('norm'):
            this_norm = {
                'meta': {},
                'paragraphs': []
            }

            """
            Norm Metadata
            """
            this_metadaten = convert_xml_to_dict(law.find('metadaten'), dict)

            # For now, Only process norms that start with §, Art, Artikel (everything else is e.g. Inhaltsverzeichnis, Anlage) (TODO)
            pattern_norm = r'(§+|Art|Artikel)\.?\s*'
            if isinstance(this_metadaten, dict) and this_metadaten.get('enbez') and re.match(pattern_norm, this_metadaten['enbez']):
                this_norm['meta'] = {
                    'norm_id': this_metadaten['enbez'],
                    'title': ''
                }
                try:
                    this_norm['meta']['title'] = law.find('metadaten').titel.text
                except AttributeError:
                    pass

                # Some laws have a "Gliederung", e.g. Art I, Art II. This would lead to duplicate titles if we ignore it
                # With this, it will look like this: Art I §1, Art II §1
                if this_metadaten.get('gliederungseinheit') and this_metadaten.get('gliederungseinheit').get('gliederungsbez'):
                    this_norm['meta']['norm_id'] = this_metadaten['gliederungseinheit']['gliederungsbez'] + ' ' + this_norm['meta']['norm_id']

                """
                Norm Content
                """
                if (law.find('textdaten') and law.find('textdaten').find('text') and law.find('textdaten').find('text').find('Content')):

                    """
                    Norm Content - P Tag (Absätze)
                    Wa want to put all Absätze in an array of paragraphs with their paragraph number.

                    Some paragraphs are numbered at the beginning of each paragraph, e.g. "(1) Die...".
                    Of those, sometimes a new P tag starts without a new number meaning it belongs to the previous paragraph.
                    For this logic, we need p_is_numbered so that we now that the paragraphs in the norm are numbered.

                    If a paragraph is not numbered, we will count ourselves with p_i.
                    """
                    this_content = law.find('textdaten').find('text').find('Content')
                    whitespace_pattern = r"\n\s+\n"  # Some paragraphs have a lot of whitespace which we will remove.
                    p_i = 0
                    p_is_numbered = False
                    for P in this_content.find_all('P', recursive=False):
                        # recursive=False so that we only get direct children (and e.g. not nested Ps such as in 'Revision' tags)
                        # Examples for laws with Revision tags: e.g. kstg § 34. Lambda e.g. bmelddav §5
                        p_i += 1
                        number = p_i
                        number_missing = False

                        # We want to check if the P tag has numbering in the beginning [(1) or 1]
                        # so that we can use it as it is more reliable then counting ourselves.
                        # However, we need to remove DL, Revision and table tags which sometimes also start with nubmers.
                        P_copy = copy.deepcopy(P)
                        for tag in P_copy.find_all(['DL', 'Revision', lambda t: t.name == 'entry' and t.get('colname') == 'col1']):
                            tag.decompose()
                        P_split = P_copy.text.split()  # We split the text at the first whitespace, leaving us with the first word.
                        # Now, we can identify the right number for the paragraph
                        if P_split:
                            first_part = P_split[0]
                            pattern_number = r"\b\d+[a-zA-Z]?\b"
                            # If the regex matches, we have a number (with optionally one letter, such as 1b)
                            match = re.search(pattern_number, first_part)
                            if match:  # If a match was found
                                number = match.group()  # Get the matched string
                                number = re.sub(r'\W+', '', number)  # Remove non-word characters (not a letter, digit)
                                p_is_numbered = True  # We now know that this norm has numbered paragraphs.

                                # Some laws have errors, e.g. BJNR048500995 § 6 has two (2).
                                # Therefore we need to check if we would add a duplicate. (TODO - optimize part)
                                for paragraph in this_norm['paragraphs']:
                                    number = str(number)
                                    if str(paragraph['meta']['paragraph_id']) == number:
                                        if bool(re.match('^\d+$', number)):
                                            number = int(number)
                                            number += 1
                                        else:
                                            number = str(number) + "_"
                                        break  # For now we're not correcting the wrong (2) at the beginning

                            # If we have not found a match, but previously did, this P tag continues the previous paragraph.
                            elif p_is_numbered:
                                number_missing = True
                                number = p_i-1
                            # If no match was found, the P has unumbered paragraphs and we will count ourselves.
                            else:
                                number = p_i

                        # Remove all SUP tags for now. Those are the little numbers in the text that refer to the sentence number (TODO).
                        for sup in P('SUP'):
                            sup.extract()

                        # This is our paragraph object that we will push to the paragraphs array.
                        # This configuration of get_text() strips all text of leading and ending whitespace
                        # and then puts all text togther separated by a whitespace.
                        p_obj = {
                            'meta': {
                                'paragraph_id': str(number),
                                'token': num_tokens_from_string(P.text)
                            },
                            'content': re.sub(whitespace_pattern, "\n\n", P.get_text(" ", strip=True))
                        }

                        # However, if the number in a numbered paragraph was missing, we will add the content to the previous paragraph.
                        if number_missing:
                            for paragraph in this_norm['paragraphs']:
                                if str(paragraph['meta']['paragraph_id']) == str(number):
                                    paragraph['meta']['token'] += p_obj['meta']['token']
                                    paragraph['content'] += " " + p_obj['content']
                                    break

                        # Otherwise, we have a new paragraph.
                        else:
                            """
                            We will now do a final check if the paragraph we want to push might be a duplicate.
                            We will go through all paragraphs of the current norm and check.
                            For example, indmeterprobv has § 3 twice, which leads to a duplicate.
                            Original: https://www.gesetze-im-internet.de/indmeterprobv/__3.html
                            Duplicate: https://www.gesetze-im-internet.de/indmeterprobv/__3_1.html
                            """
                            hard_duplicate = False
                            for norm in output['norms']:
                                if norm['meta']['norm_id'] == this_norm['meta']['norm_id']:
                                    for paragraph in norm['paragraphs']:
                                        if paragraph['meta']['paragraph_id'] == p_obj['meta']['paragraph_id']:
                                            # We found a duplicate
                                            hard_duplicate = True
                                            unprocessed_absatze.append(f"{filename} {key_process} {this_norm['meta']['norm_id']} {number}")
                                            break
                            # Only if we don't have a duplicate, we will push this paragraph.
                            if not hard_duplicate:
                                this_norm['paragraphs'].append(p_obj)

                # Pushing the fully processed norm to the output dict.
                output['norms'].append(this_norm)

        """
        Law Finish
        """
        # Add the law to the output dict
        if key_process is not None and isinstance(key_process, str) and len(key_process) > 0:
            all_laws[key_process] = output
            output = {
                'key': key_process,
                'output': output,
                'unprocessed_absatze': unprocessed_absatze
            }
            filename_without_ending = filename.split('.')[0]
            file_path_json = os.path.join(JSON_DIR_PATH, filename_without_ending)
            with open(f'{file_path_json}.json', 'w') as f:
                json.dump(output, f, ensure_ascii=False)
        else:
            print(f"Could not find amtabk or jurabk for {filename}")


def main():
    """
    Process the XML files using multiprocessing
    """

    # Initialize a Pool with the number of available processors
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    print(f"Using {multiprocessing.cpu_count()} cores/processes in parallel")

    # Processing the files with the process_file() function in parallel.
    # We are also updating a timer with tqdm.
    try:
        with tqdm(total=len(XML_FILENAMES), desc="Processing files", dynamic_ncols=True) as pbar:
            for _ in pool.imap_unordered(process_file, XML_FILENAMES):
                pbar.update()
    finally:
        pool.close()
        pool.join()

    """
    Write to JSON
    """
    print(f"Writing to {OUTPUT_FILENAME}.json ...")

    # Get all the JSON filenames in an array
    JSON_FILENAMES = [f for f in os.listdir(JSON_DIR_PATH) if f.endswith(FILE_FILTER+('.json'))]

    # Merge all JSON files to one all_json object and write that to JSON.
    # We will also store the unprocessed Absätze to write these to a file, too.
    all_json = {}
    all_unprocessed_absatze = []
    for filename in JSON_FILENAMES:
        file_path_json = os.path.join(JSON_DIR_PATH, filename)
        with open(file_path_json, encoding="utf8") as file:
            data = json.load(file)
            all_json[data['key']] = data['output']
            if data['unprocessed_absatze']:
                all_unprocessed_absatze.append(data['unprocessed_absatze'])
    with open(f'{OUTPUT_FILENAME}.json', 'w') as f:
        json.dump(all_json, f, ensure_ascii=False)

    """
    Create Analysis of results
    """
    print("Analyzing results...")
    # Store all_unprocessed_absatze to a file
    with open(f'{OUTPUT_FILENAME}_unprocessed_absatze.txt', 'w') as f:
        for item in all_unprocessed_absatze:
            f.write("%s\n" % item)

    # Get all all_json_sources we have processed
    all_json_sources = set()
    for key in all_json:
        all_json_sources.add(all_json[key]['meta']['source'])

    # Store all all_json_sources that are not in XML_FILENAMES to a file
    with open(f'{OUTPUT_FILENAME}_missing_files.txt', 'w') as f:
        for item in XML_FILENAMES:
            if item not in all_json_sources:
                f.write("%s\n" % item)

    """
    Present results
    """

    print("--- STATS ---")
    print(f"- Written to JSON {len(all_json_sources)} / {len(XML_FILENAMES)} files")
    print(f"- {len(XML_FILENAMES) - len(all_json_sources)} missing files are written to {OUTPUT_FILENAME}_missing_files.txt'")
    print(f"- {len(all_unprocessed_absatze)} unprocessed Absätze are written to {OUTPUT_FILENAME}_unprocessed_absatze.txt'")
    print("--- DONE ---")


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()

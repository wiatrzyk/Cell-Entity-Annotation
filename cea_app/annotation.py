import pandas as pd
import requests
import xmltodict
import urllib3
import Levenshtein
import pandas as pd
requests.urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def read_targets(targets_file_path, tables_folder_path):
    df_target = pd.read_csv(targets_file_path, header=None)
    df_target.columns = ["Table_id", "Column_id", "Row_id"]

    # Read all targets and find them in data tables
    df_target = df_target.assign(text=None)
    for index, row in df_target.iterrows():
        try:
            table_id = row['Table_id']
            column_id = int(row['Column_id'])
            row_id = int(row['Row_id'])
            table = pd.read_csv(f'{tables_folder_path}/{table_id}.csv', header=None)
            text = table.loc[row_id][column_id]
            df_target.loc[index, 'text'] = text
        except Exception as exc:
            print(f"EXCEPTION {exc}")
    return df_target

def clear_entity(cell_entity: str) -> str:
    chars = '!"#$%&\'()*+,./:;<=>?@[\\]^`{|}~'
    if not isinstance(cell_entity, str):
        return cell_entity
    clear_cell = ""
    for ch in cell_entity:
        if ch not in chars:
            clear_cell += ch
    return clear_cell.strip()

def check_url(enity_value: str):
    try:
        parsed_data = enity_value.replace(" ", "_")
        url = f'http://dbpedia.org/resource/{parsed_data}'
        resp = requests.get(url, headers={'Connection': 'close'})
        assert resp.status_code == 200
        return url
    except AssertionError:
        # Invalid url
        return None

    except Exception as exc:
        print(f"EXC {exc}")
        return None

def dbpedia_lookup(enity_value: str, max_hits=20):
    url = f'https://lookup.dbpedia.org/api/search/KeywordSearch?MaxHits={max_hits}&QueryString="{enity_value}"'
    # url = f'http://localhost:9274/lookup-application/api/search?query={enity_value}&maxResults={max_hits}'
    try:
        resp = requests.get(url, headers={'Connection': 'close'}, verify=False)
        if resp.status_code != 200:
            print('Service Unavailable')
            return []
        result_tree = xmltodict.parse(resp.content.decode('utf-8'))
        if result_tree['ArrayOfResults'] is None or 'Result' not in result_tree['ArrayOfResults']:
            print(enity_value)
            return []
        result = result_tree['ArrayOfResults']['Result']
        if isinstance(result, list):
            return [r['URI'] for r in result]

        return [result['URI']]
    except Exception as exc:
        print(f"EXC {exc}, {enity_value}")

def spotlight_lookup(text, lang='en', confidence=0.01):
    url = f"https://api.dbpedia-spotlight.org/{lang}/annotate"
    params = {'text': text, 'confidence':confidence}
    headers = {'accept': 'application/json'}

    try:
        response = requests.request("GET", url, headers=headers, params=params)
        results = response.json()['Resources']
        matches = []
        for result in results:
            uri = result['@URI']         
            matches.append(uri.replace('/page/', '/resource/'))
    except Exception as e:
        print(f'[SPOTLIGHT] Something went wrong with text: {text}. Returning nothing')
        return []

    return matches

# Levenshtein
def find_best_match(urls, cell_value):
    min_distance = 9999
    value = ''
    for url in urls:
        label = url.split('resource')[1][1:].replace('_', ' ')
        levenshtein_distance = Levenshtein.distance(cell_value.lower(), label.lower())
        if min_distance > levenshtein_distance:
            value = url
            min_distance = levenshtein_distance

    return value, min_distance

def get_results(cell, index, urls, dataframe):
        # Levenshtein
        best_match, distance = find_best_match(urls, cell)

        # save_results
        dataframe.loc[index, 'annotation'] = best_match
        dataframe.loc[index, 'candidates'] = str(urls)

        # print(cell, best_match, distance)
        return dataframe

def annotate(tables_folder_path, targets_file_path, check=True, lookup=True, spootlight=True, result_path=''):
    data = read_targets(targets_file_path, tables_folder_path)
    data['text'] = data['text'].map(clear_entity)
    result_df = data.assign(annotation=None, candidates=None)
    
    url_check_results = result_df.copy()
    dbpedia_lookup_results_df = result_df.copy()
    spotlight_lookup_results_df = result_df.copy()
    
    for index, row in result_df.iterrows():
        cell = row['text']
        if not isinstance(cell, str):
            continue
        
        result_candidates = []
        if check:
            # CHECK
            url = check_url(cell)
            if url is not None:
                result_candidates.append(url)
                get_results(cell, index, [url], url_check_results)
        
        if lookup:
            # DBPEDIA
            dbpedia_lookup_urls = dbpedia_lookup(cell, 10)
            if dbpedia_lookup_urls is not None:
                for url in dbpedia_lookup_urls:
                    result_candidates.append(url)
                get_results(cell, index, dbpedia_lookup_urls, dbpedia_lookup_results_df)
                
        if spootlight:
            # SPOTLIGHT
            spotlight_lookup_urls = spotlight_lookup(cell)
            if spotlight_lookup_urls is not None:
                for url in spotlight_lookup_urls:
                    if url not in result_candidates:
                        result_candidates.append(url)
                get_results(cell, index, spotlight_lookup_urls, spotlight_lookup_results_df)

        get_results(cell, index, result_candidates, result_df)
    
    return url_check_results, dbpedia_lookup_results_df, spotlight_lookup_results_df, result_df

def save_annotation_to_file(dataframe, file_path, filename = "results"):
    dataframe.to_csv(f'{file_path}/{filename}.csv', sep=',', index=False, )

if __name__ == "__main__":
    url_check_results, dbpedia_lookup_results_df, spotlight_lookup_results_df, result_df = annotate('E:\\pytong\\cell_entity_annotation_challenge\\gui\\test\\tables', "E:\\pytong\\cell_entity_annotation_challenge\\gui\\test\\test_Targets.csv")
    print(url_check_results)
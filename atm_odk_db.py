import requests
import json
from zipfile import ZipFile
from io import BytesIO, StringIO
import ray
import modin.pandas as pd
import sys
import csv_to_sqlite as csq



print("The Internet of Production Alliance.\n This program downloads form submissions from ODK central and saves the data in CSV and SQLITE formats.")


with open('credentials.json') as json_file:
    __login = json.load(json_file)

__url = __login['login']['__url']
__email = __login['login']['__email']
__password = __login['login']['__password']


def get_odk_token(url, email, password):
    email_token_response = requests.post(
        url + "/v1/sessions",
        data=json.dumps({"email": email, "password": password}),
        headers={"Content-Type": "application/json"},
    )
    if email_token_response.status_code == 200:
        print("Success getting odk token")
        return email_token_response.json()["token"]
    else:
        print("Error " + str(email_token_response.status_code))
        return False


def list_odk_projects(url, odk_token):
    response = requests.get(
        url + "/v1/projects/",
        headers={"Authorization": "Bearer " + odk_token},
    )
    if response.status_code == 200:
        print("Success getting list of projects (JSON file)")
        return response.json()
    else:
        print("Error listing projects")
        return False


def list_odk_forms(url, odk_token, project_id):
    response = requests.get(
        url + "/v1/projects/" + str(project_id) + "/forms",
        headers={"Authorization": "Bearer " + odk_token},
    )
    if response.status_code == 200:
        print("Success getting list of forms from project: " + str(project_id))
        ids = [id['xmlFormId'] for id in response.json()]
        return ids
    else:
        print("Error listing forms from project:" + str(project_id))
        return False



def download_odk_zip_submissions(url, odk_token, project_id, form_id):
    str_req = url + "/v1/projects/" + str(project_id) + "/forms/" + str(form_id) + "/submissions.csv.zip?attachments=true"
    print("Requesting: ")
    print(str_req)
    download_zip = requests.get(str_req, headers={"Authorization": "Bearer " + odk_token})
    if download_zip.status_code == 200:
        print("Success downloading ZIP file of project: " + str(project_id) + ", form: " + str(form_id))
        return download_zip.content
    else:
        print("Error " + str(download_zip.status_code))
        return False


def extract_zip_inram(zipped_file):
    __read_zip = ZipFile(BytesIO(zipped_file))
    return {__file: __read_zip.read(__file) for __file in __read_zip.namelist()}

def run_script(tool):
    if tool == "datasette":
        __token = get_odk_token(__url,__email,__password)
        if not __token:
            print("Token error")
            return False
        else:
            __projects = list_odk_projects(__url, __token)
            __projects_ids = [project_id['id'] for project_id in __projects]
            __files = {}

            for __project_id in __projects_ids:
                __form_ids = list_odk_forms(__url, __token, __project_id)
                __files[str(__project_id)] = {}
                
                for __form_id in __form_ids:
                    __files[str(__project_id)][__form_id] = {}
                    __form_data = __files[str(__project_id)][__form_id]
                    __form_data['download'] = download_odk_zip_submissions(__url, __token, __project_id, __form_id)
                    __form_data['zips'] = extract_zip_inram(__form_data['download'])
                    __form_data['files'] = {}

                    for __file in __form_data['zips'].items():
                        __form_data['files'][__file[0]] = pd.read_csv(StringIO(__file[1].decode('utf-8')))
                    
                    for __count, __csv in enumerate(__form_data['files'].values(), start=0):
                        try:
                            __next = list(__form_data['files'].values())[__count +1]
                            __merged = pd.merge(__csv, __next, left_on='KEY', right_on='PARENT_KEY', how='right') #.drop('PARENT_KEY', axis=1)
                            print("Success mergin dataframes by KEY, PARENT_KEY")
                        except IndexError:
                            break
                
                __file_name = "./bin/" + str(__project_id) + "_"+ __form_id
                __merged.to_csv(__file_name + ".csv")

                __options = csq.CsvOptions(typing_style="full", encoding="UTF-8")
                csq.write_csv([__file_name], __file_name +".sqlite", __options)

            print("Successfully written csv and sqlite files")
            
    else:
        return False


if __name__ == "__main__":
    ray.init()
    if not run_script("datasette"):
        sys.exit()

else:
    print(__name__ + " is imported as a Module")

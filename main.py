#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
import base64
import pandas as pd
import logging
import hashlib
from unidecode import unidecode
from dotenv import dotenv_values

logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = dotenv_values(".env")
api_token = config["CKAN_API_KEY"]
ckan_api_url = config["CKAN_API_URL"]

def clean_servidor(filepath):
    d = pd.read_csv(filepath)
    del d['nome_servidor']
    d['numero_matricula'] = d['numero_matricula'].apply(lambda d: hashlib.md5(str(d).encode()).hexdigest())
    d['data_nascimento'] = d['data_nascimento'].apply(lambda d: d.split('/')[-1])
    d.to_csv(filepath, index=False)

memory_api_endpoints = {
    "Pessoal": {
        "organization": "secretaria-de-administracao",
        "endpoints": [
            {
                "name": "Servidor",
                "url_name": "servidor",
                "process": clean_servidor,
                "url": "https://publico.memory.com.br/curvelo/lai/pessoal/servidor/exportar?page=1&size=9999&type=csv",
                "filename": "servidor-$exercio$.csv",
                "headers": {
                    "tenant-id": "99K7P2",
                    "entidade": "2",
                    "exercicio": [2023, 2024]
                }
            },
            {
                "name": "Diaria de viagem",
                "url_name": "diaria_de_viagem",
                "url": "https://publico.memory.com.br/curvelo/lai/pessoal/servidor/exportar?page=1&size=9999&type=csv",
                "filename": "diaria-de-viagem-$exercio$.csv",
                "headers": {
                    "tenant-id": "99K7P2",
                    "entidade": "2",
                    "exercicio": [2023, 2024]
                }
            }
        ]
    },
    "Contabilidade": {
        "organization": "secretaria-de-fazenda",
        "endpoints": [{
            "name": "Convênios",
            "url_name": "convenios",
            "url": "https://publico.memory.com.br/curvelo/lai/contabilidade/convenio/exportar?page=1&size=9999&type=csv",
            "filename": "convenios-$exercio$.csv",
            "headers": {
                "tenant-id": "99K7P2",
                "entidade": "2",
                "exercicio": [2023, 2024]
            }
        }]
   },
    # "Compras": {
    #     "organization": "secretaria-de-fazenda",
    #     "endpoints": [{
    #         "name": ""
    #         "url_name": ""
    #         "url": "https://publico.memory.com.br/curvelo/lai/contabilidade/convenio/exportar?page=1&size=9999&type=csv",
    #         "filename":  "compras-$exercio$.csv",
    #         "headers": {
    #             "tenant-id": "99K7P2",
    #             "entidade": "2",
    #             "exercicio": [2023, 2024]
    #         }
    #     }]
    # }
}

def create_package(api_token, owner_org, package_title, package_name, package_description = ""):
    request_data = {
      "owner_org": "financas",
      "name": unidecode(package_name),
      "title": package_title
    }
    
    headers = {
      "Authorization": api_token
    }

    result = requests.post("https://curvelo.vaz.io/api/action/package_create", headers = headers, json = request_data)
    resp_dict = json.loads(result.content)
    return resp_dict

def check_package(package_name):
    resp = requests.get(f"{ckan_api_url}/api/3/action/package_show?id={package_name}")
    resp_dict = json.loads(resp.content)
    if resp_dict["success"]:
        return {"package_id": resp_dict["result"]["id"]}
    else:
        return False

def check_resource(resource_name):
    resp = requests.get(f"{ckan_api_url}/api/3/action/resource_search?query=name:{resource_name}")
    resp_dict = json.loads(resp.content)
    if resp_dict["success"] and len(resp_dict["result"]["results"]) > 0:
        return { "resource_id": resp_dict["result"]["results"][0]["id"] }
    else:
        return False

def upsert_resource(api_token, resource_name, resource_url_name, package_id, filepath, resource_id=''):
    if resource_id != '':
        resource_api = f"{ckan_api_url}/api/action/resource_patch"
        request_data = {
            "id": resource_id
        }
    else:
        resource_api = f"{ckan_api_url}/api/action/resource_create"
        request_data = {
          "package_id": package_id,
          "name": resource_url_name,
          "title": resource_name
        }
    
    headers = {
      "Authorization": api_token
    }
    
    resultado = requests.post(resource_api,
                              headers = headers,
                              data = request_data,
                              files = [('upload', open(filepath, 'rb'))]
                             )
    resposta_dict = json.loads(resultado.content)
    return resposta_dict

def fetch_data(endpoint, exercicio):
    endpoint["headers"]["exercicio"] = str(exercicio)
    filename = endpoint["filename"].replace("$exercio$", str(exercicio))
    resp = requests.get(endpoint["url"], headers=endpoint["headers"])
    logger.warning(f"Endpoint downloaded")
    resp_data = json.loads(resp.content)
    filepath = '/tmp/' + filename
    file = open(filepath, 'wb')

    if "path" in resp_data:
        decoded_data = base64.b64decode(resp_data["path"])
        file.write(decoded_data)
        file.close()

        return filepath
    else:
        return False

def main():
    for endpoints in memory_api_endpoints.values():
        organization = endpoints["organization"]
        for e in endpoints["endpoints"]:
            # Check package exist
            package = check_package(e["url_name"])
    
            package_id = ''
            if not package:
                resp = create_package(api_token, organization, e["name"], e["url_name"])
                if resp["success"]:
                    package_id = resp["result"]["id"]
            else:
                package_id = package["package_id"]
    
            # Get the data
            for year in e["headers"]["exercicio"]:
                logger.warning("Download the data for " + e["url"])
                filepath = fetch_data(e, year)
    
                # Upload the resource
                if filepath:
                    resource_url_name = unidecode(f'{e["name"]} {year}')
                    resource_url_name = resource_url_name.replace(' ', '_')
                    resource = check_resource(resource_url_name)

                    if 'process' in e:
                        e['process'](filepath)
                    
                    if resource:
                        resp = upsert_resource(api_token, e["name"], resource_url_name, package_id, filepath, resource["resource_id"])
                    else:
                        resp = upsert_resource(api_token, e["name"], resource_url_name, package_id, filepath)


if __name__ == '__main__':
    main()


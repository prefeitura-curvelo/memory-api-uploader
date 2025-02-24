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
        "organization": "secretaria-municipal-de-administracao-politicas-sociais-e-desenvolvimento-sustentavel",
        "endpoints": [
             {
                 "name": "Servidor",
                 "url_name": "servidor",
                 "notes": "Relação nominal de servidores, idealmente incluindo aposentados e pensionistas, com informações sobre cargos ocupados e as respectivas remunerações.",
                 "title": "Teste",
                 #"process": clean_servidor,
                 "url": "https://publico.memory.com.br/curvelo/lai/pessoal/servidor/?page=1&size=9999",
                 "filename": "servidor-$exercio$.csv",
                 "headers": {
                     "tenant-id": "99K7P1",
                     "entidade": "1",
                     "exercicio": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
                 }
             },
            {
                "name": "Diaria de viagem",
                "url_name": "diaria_de_viagem",
                "notes": "Relação das Diárias de viagem dos servidores municipais.",
                "url": "https://publico.memory.com.br/curvelo/lai/pessoal/diariasdeviagem/?page=1&size=9999",
                "filename": "diaria-de-viagem-$exercio$.csv",
                "headers": {
                    "tenant-id": "99K7P1",
                    "entidade": "1",
                    "exercicio": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
                }
             },
             {
                 "name": "Gasto com pessoal",
                 "url_name": "gasto_com_pessoal",
                 "notes": "Informações detalhadas sobre os gastos com pessoal e abate teto.",
                 "url": "https://publico.memory.com.br/curvelo/lai/pessoal/servidor/abateteto?page=1&size=9999",
                 "filename": "gasto-com-pessoal-$exercio$.csv",
                 "headers": {
                     "tenant-id": "99K7P1",
                     "entidade": "1",
                     "exercicio": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],
                     "mesano": "1"
                 }
            }
        ]
    },
     "Contabilidade": {
         "organization": "secretaria-de-fazenda",
         "endpoints": [{
             "name": "Convênios",
             "url_name": "convenios",
             "notes": "Relação de contratos, convênios e parcerias, incluindo quem e o que contratou, informações sobre a contratada/parceira, valores, data e período de contratação e, idealmente, modalidade e informações sobre o certame que a originou.",
             "url": "https://publico.memory.com.br/curvelo/lai/contabilidade/convenio/?page=1&size=9999",
             "filename": "convenios-$exercio$.csv",
             "headers": {
                 "tenant-id": "99K7P1",
                 "entidade": "1",
                 "exercicio": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
             }
         }]
    },
     "Compras": {
         "organization": "secretaria-de-fazenda",
         "endpoints": [
         {
             "name": "Obras públicas",
             "url_name": "obras-publicas",
             "notes": "Relação de projetos, idealmente incluindo aqueles não aprovados e em análise além dos aprovados, com geolocalização, status e outras informações, como tipo, empresa responsável e contratos relacionados.",
             "url": "https://publico.memory.com.br/curvelo/lai/obraspublicas/?page=1&size=9999",
             "filename":  "obras-publicas-$exercio$.csv",
             "headers": {
                 "tenant-id": "99K7P1",
                 "entidade": "1",
                 "exercicio": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
             }
         },
         {
             "name": "Compras Diretas",
             "url_name": "compra-direta",
             "notes": "Relação de contratos, convênios e parcerias, incluindo quem e o que contratou, informações sobre a contratada/parceira, valores, data e período de contratação e, idealmente, modalidade e informações sobre o certame que a originou.",
             "url": "https://publico.memory.com.br/curvelo/lai/obraspublicas/?page=1&size=9999",
             "filename":  "compra-direta-$exercio$.csv",
             "headers": {
                 "tenant-id": "99K7P1",
                 "entidade": "1",
                 "exercicio": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
             }
         },
         {
             "name": "NAF's",
             "url_name": "nafs",
             "notes": "Relação de contratos, convênios e parcerias, incluindo quem e o que contratou, informações sobre a contratada/parceira, valores, data e período de contratação e, idealmente, modalidade e informações sobre o certame que a originou.",
             "url": "https://publico.memory.com.br/curvelo/lai/compras/naf/?page=1&size=9999",
             "filename":  "nafs-$exercio$.csv",
             "headers": {
                 "tenant-id": "99K7P1",
                 "entidade": "1",
                 "exercicio": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
             }
         },
         {
             "name": "Contratos",
             "url_name": "contratos",
             "notes": "Relação de contratos, convênios e parcerias, incluindo quem e o que contratou, informações sobre a contratada/parceira, valores, data e período de contratação e, idealmente, modalidade e informações sobre o certame que a originou.",
             "url": "https://publico.memory.com.br/curvelo/lai/compras/contrato/?page=1&size=9999",
             "filename":  "contratos-$exercio$.csv",
             "headers": {
                 "tenant-id": "99K7P1",
                 "entidade": "1",
                 "exercicio": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
             }
         },
        {
            "name": "Editais",
            "url_name": "editais",
            "notes": "Relação dos processos licitatórios, incluindo chamadas e editais concluídos e em andamento, com informações sobre o número da licitação, o objeto, a modalidade, a data de abertura, o ano de exercício, status atual e, idealmente, os concorrentes e o ganhador.",
            "url": "https://publico.memory.com.br/curvelo/lai/compras/edital/?page=1&size=9999",
            "filename":  "edital-$exercio$.csv",
            "headers": {
                "tenant-id": "99K7P1",
                "entidade": "1",
                "exercicio": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
            }
        },
        {
            "name": "Licitações",
            "url_name": "licitacoes",
            "notes": "Relação dos processos licitatórios, incluindo chamadas e editais concluídos e em andamento, com informações sobre o número da licitação, o objeto, a modalidade, a data de abertura, o ano de exercício, status atual e, idealmente, os concorrentes e o ganhador.",
            "url": "https://publico.memory.com.br/curvelo/lai/compras/licitacao/?page=1&size=9999",
            "filename":  "licitacao-$exercio$.csv",
            "headers": {
                "tenant-id": "99K7P1",
                "entidade": "1",
                "exercicio": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
            }
        }
         ]
     }
}

def create_package(api_token, owner_org, package_title, package_name, package_description):
    request_data = {
      "owner_org": owner_org,
      "name": unidecode(package_name),
      "notes": package_description,
      "title": package_title
    }
    
    headers = {
      "Authorization": api_token
    }

    result = requests.post(f"{ckan_api_url}/api/action/package_create", headers = headers, json = request_data)
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

# def fetch_data(endpoint, exercicio):
#     endpoint["headers"]["exercicio"] = str(exercicio)
#     filename = endpoint["filename"].replace("$exercio$", str(exercicio))
#     resp = requests.get(endpoint["url"], headers=endpoint["headers"])
#     logger.warning(f"Endpoint downloaded: {endpoint["name"]} OK")
#     resp_data = json.loads(resp.content)
#     filepath = '/tmp/' + filename
#     file = open(filepath, 'wb')

#     if "data" in resp_data:
#         data_list = resp_data.get("data", [])
#         df = pd.DataFrame(data_list)
#         df = df.replace('\r\n', '', regex=True)
#         df.to_csv(filepath, index=False)
#         return filepath

#     else:
#         return False

def fetch_data(endpoint, exercicio):
    filename = endpoint["filename"].replace("$exercio$", str(exercicio))
    filepath = '/tmp/' + filename

    # Verifica se o endpoint é "Gasto com pessoal" para buscar os 12 meses
    if endpoint["url_name"] == "gasto_com_pessoal":
        all_data = []
        for month in range(1, 13):  # De 1 a 12 para cada mês
            headers = endpoint["headers"].copy()
            headers["exercicio"] = str(exercicio)
            headers["mesano"] = str(month)
            logger.info(f"Consultando {endpoint['name']} para {exercicio}/{month}")
            resp = requests.get(endpoint["url"], headers=headers)
            resp_data = json.loads(resp.content)
            if "data" in resp_data and resp_data["data"]:
                all_data.extend(resp_data["data"])
            else:
                logger.warning(f"Nenhum dado encontrado para {exercicio}/{month}")

        if not all_data:
            return False
        
        df = pd.DataFrame(all_data)
        df = df.replace('\r\n', '', regex=True)
        df.to_csv(filepath, index=False)
        return filepath
    else:
        # Comportamento original para outros endpoints
        endpoint["headers"]["exercicio"] = str(exercicio)
        resp = requests.get(endpoint["url"], headers=endpoint["headers"])
        logger.warning(f"Endpoint downloaded: {endpoint['name']} OK")
        resp_data = json.loads(resp.content)
        filepath = '/tmp/' + filename
        file = open(filepath, 'wb')

        if "data" in resp_data:
            data_list = resp_data.get("data", [])
            df = pd.DataFrame(data_list)
            df = df.replace('\r\n', '', regex=True)
            df.to_csv(filepath, index=False)
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
                resp = create_package(api_token, organization, e["name"], e["url_name"], e["notes"])
                if resp["success"]:
                    package_id = resp["result"]["id"]
            else:
                package_id = package["package_id"]
    
            # Get the data
            for year in e["headers"]["exercicio"]:
                logger.warning(f"Download the data for {e['url']} in {year}")

                try:
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
                except Exception as ex:
                    logger.warning(f"Error downloading data for {e['name']} in {year}: {str(ex)}")

if __name__ == '__main__':
    main()

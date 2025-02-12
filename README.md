# Script paras baixar dados da API da Memory e subir num servidor CKAN

Instalar as dependências em um ambiente virtual com:

```
python -m venv ckan-uploader
source ckan-uploader/bin/activate
pip install -r requirements.txt
```

Edit o conteúdo do arquivo `.env` com o endereço do seu servidor CKAN e com o CKAN api token.

Rode o script com:

```
python main.py
```

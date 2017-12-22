import datetime as dt
import requests
from pymongo import MongoClient, UpdateOne

connection_string = 'mongodb://personal:connectionstring' # CHANGE THIS
db = MongoClient(connection_string)['camara'] # 'camara' é o nome da DB criada para armazenar os dados

legis_dates = {} # inicialmente coletando dados de início e término das legislaturas
for legis in range(52, 56):
    legis_data = requests.get('https://dadosabertos.camara.leg.br/api/v2/legislaturas/{}'.format(legis)).json()['dados']
    legis_dates.update({legis: {'dataInicio': dt.datetime.strptime(legis_data['dataInicio'], '%Y-%m-%d'), 'dataFim': dt.datetime.strptime(legis_data['dataFim'], '%Y-%m-%d')}})

legislaturas = ','.join([str(n) for n in range(52, 56)])
url = 'https://dadosabertos.camara.leg.br/api/v2/deputados/?idLegislatura={}&itens=100'.format(legislaturas)

while True:
    r = requests.get(url)
    d = r.json()
    
    actions = [] # iniciando conjunto de updates para inserir no Mongo de forma mais eficiente via bulk_write
    
    for parl in d['dados']:
        uri = parl['uri'] # acessando a página específica do deputado com dados cadastrais
        cadastro = requests.get(uri).json()['dados']
        campos = ['nomeCivil', 'sexo', 'ufNascimento', 'municipioNascimento']
        set_dict = {
            'nome': parl['nome'], # em alguns casos há disparidade de nome. aqui garantimos que o usado é a legislatura mais recente                 
        }
        
        for campo in campos:
            if cadastro[campo]:
                set_dict.update({campo: cadastro[campo]}) # evitando inserir campos com valor nulo
        
        try:
            dataNascimento = dt.datetime.strptime(cadastro['dataNascimento'], '%Y-%m-%d')
            set_dict.update({'dataNascimento': dataNascimento})
        except:
            pass
        try:
            dataFalecimento = dt.datetime.strptime(cadastro['dataFalecimento'], '%Y-%m-%d')
            set_dict.update({'dataFalecimento': dataFalecimento})
        except:
            pass
        
        actions.append(UpdateOne(
            {
                'idParlamentar': parl['id'] # checando identidade
            },{
                '$push': {'mandatos': {'legislatura': parl['idLegislatura'], 
                                       'partido': parl['siglaPartido'], 
                                       'uf': parl['siglaUf'],
                                       'dataInicio': legis_dates[ parl['idLegislatura']]['dataInicio'],
                                       'dataFim': legis_dates[ parl['idLegislatura']]['dataFim']
                                      }}, # atualizando array de mandatos
                '$set': set_dict
            },
            upsert=True # caso não exista ainda na base o parlamentar em questão, inserir um novo documento
        ))
        
    result = db.parlamentares.bulk_write(actions)

    url = '' # checando se há ao menos mais uma página com dados a serem consumidos
    for link in d['links']:
        if link['rel'] == 'next':
            url = link['href']
            break
    if not url:
        break


import json
import requests
import boto3

from bs4 import BeautifulSoup

from dateutil.parser import parse
from datetime import datetime, timedelta
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

#+-------------------------------------------------------------------------+
#|                   FUNÇÕES DO PYTHON_SCRAPER.PY                          |
#+-------------------------------------------------------------------------+


def decide_path_by_date(**context):

    """
        Decide o caminho da DAG verificando se já há arquivos JSON para a execution_date no S3.

        Se houver JSON no S3, segue para leitura. Caso contrário, coleta os dados da API.
    """

    execution_date = context['execution_date'].astimezone()
    folder_date = execution_date.strftime('%Y%m%d')
    file_date = execution_date.strftime('%Y%m%d_%H%M%S')
    prefix = f'arquivos/{folder_date}/'

    hook = S3Hook( aws_conn_id='aws_conn' )
    bucket_name = 'personal-spotify-wrapped'

    keys = hook.list_keys( bucket_name=bucket_name, prefix=prefix )

    if keys:
        for key in keys:
            if key.endswith('.json') and file_date in key:
                print(f'[INFO] Arquivo para a execution_date {execution_date} encontrado: {key}.')
                return ['check_s3_folder']

    print(f'[INFO] Nenhum arquivo encontrado para {execution_date}.')
    return ['create_s3_folder_if_not_exists']

def refresh_spotify_token():

    """
    Atualiza o token de acesso da API do Spotify usando o refresh_token

    """

    url = 'https://accounts.spotify.com/api/token'

    access_token = Variable.get( 'access_token' )
    refresh_token = Variable.get( 'refresh_token' )
    client_id = Variable.get( 'client_id' )
    date_token = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    client_secret = Variable.get( 'client_secret' )


    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }

    print(data)

    response = requests.post( url, data=data )

    if response.status_code == 200:

        access_token = response.json().get( 'access_token' )
        refresh_token = response.json().get( 'refresh_token' )
        Variable.set( 'access_token', access_token )
        Variable.set( 'refresh_token', refresh_token )

        print( f'Novo access_token obtido. {date_token}')

    else:

        raise AirflowFailException( 'Falha ao obter o access_token' )


def get_spotify_history( **kwargs ):

    """
    Coleta as 50 últimas músicas reproduzidas recentemente na API do Spotify.

    Os dados coletados são salvos em um arquivo JSON no S3, com base na execution_date.
    """

    url = 'https://api.spotify.com/v1/me/player/recently-played'
    limit = 50

    date_now = kwargs['next_execution_date'].in_timezone( 'America/Sao_Paulo' )
    before = int(round(date_now.timestamp() * 1000))

    access_token = Variable.get( 'access_token' )

    print( f'Execution Date: {date_now}' )

    params = {
        'limit': limit,
        'before': before
    }

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get( url, params=params, headers=headers )

    if response.status_code == 200:

        data = response.json()['items']

        date_now = kwargs['next_execution_date'].in_timezone( 'America/Sao_Paulo' )


        save_json_to_s3( data, date_now)
    else:

        raise AirflowFailException( 'Falha ao tentar obter o histórico de músicas' )

def save_json_to_s3( data, date ):

    """
    Salva os dados de faixas do Spotify em formato JSON no bucket S3.

    """
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=Variable.get( 'aws_access_key_id' ),
        aws_secret_access_key=Variable.get( 'aws_secret_access_key' ),
        region_name=Variable.get( 'aws_region' )
    )

    date = date.strftime( '%Y%m%d_%H%M%S' )
    bucket_name = Variable.get( 's3_bucket_name')
    filename = f'tracks_history_{date}.json'
    date_folder = date.split('_')[0]
    file_path = f'arquivos/{date_folder}/{filename}'

    print( f' Data Pasta: {date_folder}' )

    json_data = json.dumps( data, indent=4 )

    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_path,
        Body=json_data,
        ContentType='application/json'
    )

    print( f'Arquivo salvo no S3: {bucket_name}/{file_path}' )

def create_s3_folder_if_not_exists( **kwargs ):

    """
    Verifica se a pasta correspondente à data de execução existe no bucket S3.

    Caso não exista, cria a pasta para armazenar os arquivos JSON.
    """

    hook = S3Hook( aws_conn_id='aws_conn' )
    bucket_name = 'personal-spotify-wrapped'

    folder_name = kwargs['next_execution_date'].strftime( '%Y%m%d' )
    folder_path = f'arquivos/{folder_name}/'

    if hook.check_for_prefix( bucket_name=bucket_name, prefix=folder_path, delimiter='/' ):

        print( f"A pasta '{folder_name}' existe no bucket '{bucket_name}'." )

    else:

        hook.get_conn().put_object(
            Bucket=bucket_name,
            Key=folder_path
        )

        print( f"A pasta '{folder_name}' for criada com sucesso no bucket '{bucket_name}'." )


#+-----------------------------------------------------------------------------+
#|                    FUNÇÕES DO CONSOLIDACAO_SPOTIFY.PY                       |
#+-----------------------------------------------------------------------------+

def check_s3_folder ( **kwargs ):

    hook = S3Hook( aws_conn_id='aws_conn' )
    bucket_name = 'personal-spotify-wrapped'

    date_now = kwargs['next_execution_date'].in_timezone( 'America/Sao_Paulo' )
    date = date_now.strftime('%Y%m%d')

    prefix = f'arquivos/{date}/'

    print( f'        A PASTA QUE ESTOU PROCURANDO É A {prefix}' )

    if hook.check_for_prefix(bucket_name=bucket_name, prefix=prefix, delimiter='/'):
        
        files_bucket = hook.list_keys(bucket_name=bucket_name, prefix=prefix, max_items=30)

        files = [file for file in files_bucket if file.endswith('.json')]

        kwargs['ti'].xcom_push(key="json_files", value=files)

        print(files)

    else:
        print(f'Não foi encontrada a pasta {prefix} no AWS S3')

def open_json_files(**context):

    """
    Lê os arquivos JSON salvos no S3 e insere os dados de reprodução no DynamoDB.

    Verifica se já existe o item com base no campo 'played_at' antes de inserir.
    """

    aws_hook = AwsBaseHook(aws_conn_id="aws_conn", client_type="dynamodb")
    dynamodb_client = aws_hook.get_client_type()

    hook = S3Hook(aws_conn_id="aws_conn")

    serializer = TypeSerializer()

    bucket_name = 'personal-spotify-wrapped'
    table_name = "spotify_tracks_history"
    

    files = context["task_instance"].xcom_pull(
        task_ids='check_s3_folder', key='json_files'
    )

    if not files:
        raise AirflowFailException("Nenhum arquivo JSON foi retornado por check_s3_folder")
    
    for file in files:

        print(file)

        file_content = hook.read_key(
            key=file,
            bucket_name='personal-spotify-wrapped'
        )

        json_data = json.loads(file_content)

        for track in json_data:

            played_at = track.get('played_at')
            dt = datetime.fromisoformat(played_at.replace('Z', ''))

            date_played = dt.strftime('%Y-%m-%d') # Partition Key
            hour_played = dt.strftime('%H:%M:%S') # Sort Key

            musica = track['track']['name']
            artista = track['track']['artists'][0]['name']

            existing_item = dynamodb_client.get_item(
                TableName = table_name,
                Key={
                    'date_played': {'S': date_played},
                    'hour_played': {'S': hour_played}
                }
            )

            if "Item" not in existing_item:

                dynamo_item_dict = {
                    'date_played': date_played,
                    'hour_played': hour_played,
                    'played_at': played_at,
                    'track': track['track'],
                    'context': track.get('context')
                }

                dynamo_item = {k: serializer.serialize(v) for k, v in dynamo_item_dict.items()}

                dynamodb_client.put_item(
                    TableName = table_name,
                    Item=dynamo_item
                )

                print(f'Adicionado: {musica} - {artista} em {played_at}')

            else:
                print(f'Já existe o registro da música {musica} executada em {played_at} no banco de dados')


#+-------------------------------------------------------------------------+
#|             RETIRAR DO DYNAMO E COLOCAR NO RDS                          |
#+-------------------------------------------------------------------------+

def extract_tracks_from_dynamodb( **kwargs ):

    aws_hook = AwsBaseHook( aws_conn_id='aws_conn', client_type='dynamodb' )
    dynamo_client = aws_hook.get_client_type()
    table_name = 'spotify_tracks_history'

    execution_date = kwargs['execution_date'].strftime( '%Y-%m-%d' )

    response = dynamo_client.query(
        TableName=table_name,
        KeyConditionExpression='date_played = :date',
        ExpressionAttributeValues={
            ":date": {'S': execution_date}
        }
    )

    items = clean_dynamodb_items( response['Items'] )

    items.sort( key=lambda x: parse(x['played_at']))

    for idx, item in enumerate(items):
        track = item['track']
        played_at = parse(item['played_at'])

        duration_ms = track.get('duration_ms', 0)
        playback_sec = int(duration_ms) // 1000
        item['playback_sec'] = playback_sec

        if idx < len(items) - 1:
            next_played_at = parse( items[idx + 1]['played_at'])
            intervalo_seg = (next_played_at - played_at).total_seconds()

            item['was_played'] = intervalo_seg >= float(playback_sec) * 0.9 # margem de 90%
            item['playback_sec'] = min(intervalo_seg, float(playback_sec))
        else:
            item['was_played'] = True 

    kwargs['ti'].xcom_push( key='tracks', value=items )

def download_previews( **kwargs ):

    items = kwargs['ti'].xcom_pull( key='tracks', task_ids='extract_tracks' )

    preview = dict()

    for item in items:
        
        track_id = item['track']['id']
        embed_url = f'https://open.spotify.com/embed/track/{track_id}'

        headers = {"User-Agent": "Mozilla/5.0"}

        response = requests.get( embed_url, headers=headers )

        if response.status_code != 200:
            print( f'Erro ao acessar {embed_url}' )
            continue

        soup = BeautifulSoup(response.text, "html.parser")

        script_tag = soup.find( "script", {"id": "__NEXT_DATA__"})

        if not script_tag:
            print('Script __NEXT_DATA__ não encontrado' )
            continue


        try:
            data = json.loads(script_tag.string)
            preview_url = data["props"]["pageProps"]["state"]["data"]["entity"]["audioPreview"]["url"]

        except Exception as e:
            print( f'Erro ao extrair preview de {track_id}: {e}' )
            continue

        # Request para baixar o preview
        preview_response = requests.get( preview_url )
        
        if preview_response.status_code == 200:
            file_path = f'/tmp/{track_id}.mp3'
            with open( file_path, 'wb' ) as f:
                f.write( preview_response.content )
            preview[track_id] = file_path
    
    kwargs['ti'].xcom_push( key='preview_files', value=preview )

def return_data_by_track_id( track_id, spotify_items ):

    for item in spotify_items:

        if item['track']['id'] == track_id:

            return item
        
    return {}

def extract_audio_features( **kwargs ):

    previews = kwargs['ti'].xcom_pull( key='preview_files', task_ids='download_previews' )
    spotify_items = kwargs['ti'].xcom_pull( key='tracks', task_ids='extract_tracks' )

    recco_endpoint = "https://api.reccobeats.com/v1/analysis/audio-features"

    headers = { 'Accept': 'application/json' }
 
    pg_hook = PostgresHook( postgres_conn_id='spotify-postgres' )

    features_data = {}

    for item in spotify_items:

        track = item['track']
        track_id = track['id']

        duration_ms = track.get('duration_ms', 0)
        playback_sec = item.get('playback_sec') or duration_ms // 1000
        was_played = item.get('was_played', True)
        played_at = item.get('played_at')

        if verificar_features_extraidas(track_id, pg_hook):
            print( f'[INFO] Track {track_id} já possui features. Pulando extração.' )

            features_data[played_at] = {
                'track': track,
                'album': track.get('album', {}),
                'artists': track.get('artists', []),
                'played_at': played_at,
                'playback_sec': playback_sec,
                'was_played': was_played,
                'audio_features': {}
            }
            continue

        path = previews.get(track_id)
        if not path:
            print(f'[WARN] preview não encontrado para a track {track_id} reproduzida em {played_at}, salvando sem features.')

            features_data[played_at] = {
                'track': track,
                'album': track.get('album', {}),
                'artists': track.get('artists', []),
                'played_at': played_at,
                'playback_sec': playback_sec,
                'was_played': was_played,
                'audio_features': {}
            }
            continue
            
        
        try:
            with open( path, 'rb' ) as f:
                
                files = {'audioFile': f}
                response = requests.post( recco_endpoint, files=files, headers=headers)

        except Exception as e:
            print( f'[ERROR] Falha ao abrir arquivo {path}: {e}' )
            continue

        if response.status_code != 200:

            print( f'[ERROR] Falha na extração de features para {track_id}. Status {response.status_code}' )
            continue

        # Obtem as features da API
        audio_features = response.json()

        features_data[played_at] = {
            'track': track,
            'album': track.get('album', {}),
            'artists': track.get('artists', []),
            'played_at': played_at,
            'playback_sec': playback_sec,
            'was_played': was_played,
            'audio_features': audio_features
        }

    kwargs['ti'].xcom_push( key='features_data', value=features_data )

def insert_into_postgres( **kwargs ):

    """
    Insere os dados dos tracks no banco de dados PostgreSQL

    dados_tracks: lista de dicionários contendo dados do DynamoDB + API do ReccoBeats
    """

    hook = PostgresHook( postgres_conn_id='spotify-postgres' )
    conn = hook.get_conn()
    cursor = conn.cursor()

    dados_tracks = kwargs['ti'].xcom_pull( key='features_data', task_ids='extract_audio_features' )

    for played_at, item in dados_tracks.items():

        track = item.get('track')
        track_id = track.get('id')
        album = item.get('album', {})
        artists = item.get('artists', [])
        features = item.get('audio_features', {}) or {}

        # Artistas
        for artist in artists:

            artist_id = artist['id']

            img, popularity, followers = get_artist_data(artist_id)

            cursor.execute(
            """
                INSERT INTO artist (artist_id, name, image_url, popularity, followers)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (artist_id) DO NOTHING;
            """,
            (artist['id'], artist['name'], img, popularity, followers)
            )

        # Album
        album_image = None

        release_date = ''
        precision = ''

        if isinstance( album, dict ):
            
            if album and isinstance( album.get('images'), list):
                album_image = next( ( img['url'] for img in album['images'] if img.get( 'width') == 300 ), None )

            release_date = album.get('release_date')
            precision = album.get('release_date_precision')
        else:
            album_image = ''
            release_date = ''
            precision = ''

        if release_date:
            if precision == 'year':
                release_date = f'{release_date}-01-01'
            elif precision == 'month':
                release_date = f'{release_date}-01'

        cursor.execute(
            """
                INSERT INTO album (album_id, name, release_date, total_tracks, album_type, image_url)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (album_id) DO NOTHING;
            """,
            (
                album.get('id'),
                album.get('name'),
                release_date,
                album.get('total_tracks'),
                album.get('album_type'),
                album_image
            )
        )

        # Track
        cursor.execute(
        """
            INSERT INTO track (
                track_id, name, duration_ms, uri, album_id, explicit, popularity,
                acousticness, danceability, energy, instrumentalness,
                liveness, speechiness, valence, tempo
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (track_id) DO NOTHING;
        """,
        (
            track['id'], track['name'], track['duration_ms'], track['uri'],
            album.get('id'), track.get('explicit'), track.get('popularity'),
            features.get('acousticness'), features.get('danceability'), features.get('energy'),
            features.get('instrumentalness'), features.get('liveness'), features.get('speechiness'),
            features.get('valence'), features.get('tempo')
        ))


        # Track_Artist
        for artist in artists:

            cursor.execute(
            """
                INSERT INTO track_artist (track_id, artist_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """,
            (
                track['id'], artist['id']
            ))

        
        # Playback History
        played_at = item.get('played_at')
        playback_sec = item.get('playback_sec')
        was_played = item.get('was_played', True)

        cursor.execute(
        """
            INSERT INTO playback_history (track_id, played_at, playback_sec, was_played, popularity)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (played_at) DO NOTHING;
        """,
        (
            track['id'], played_at,
            playback_sec, was_played,
            track.get('popularity')
        ))

    conn.commit()
    cursor.close()
    conn.close()

def clean_dynamodb_items( items ):

    deserializer = TypeDeserializer()

    return [ {k: deserializer.deserialize(v) for k, v in item.items()} for item in items ]

def verificar_features_extraidas( track_id, pg_hook ):
    """
        Verificar se os audio_features da track já foram extraídos anteriormente.
    """
    sql = "SELECT 1 FROM track WHERE track_id = %s LIMIT 1"
    result = pg_hook.get_first( sql, parameters=( track_id, ) )
    return result is not None

def get_artist_data(artist_id):

    def _request(token):
        headers= {"Authorization": f"Bearer {token}"}
        url = f'https://api.spotify.com/v1/artists/{artist_id}'
        return requests.get(url, headers=headers)

    access_token = Variable.get("access_token")
    response = _request(access_token)

    if response.status_code == 401:
        print("[WARN] Token expirado. Fazendo refresh...")
        refresh_spotify_token()
        access_token = Variable.get("access_token")
        response = _request(access_token)

    if response.status_code != 200:
        raise Exception(f"Erro ao buscar artista {artist_id}: {response.status_code} {response.text}")
    
    data = response.json()
    name = data['name']

    images = data.get('images', [])
    img_url = images[-1]['url'] if images else None

    popularity = data.get('popularity')
    followers = data.get('followers', {}).get('total')

    return img_url, popularity, followers

dag = DAG(  
        dag_id = "spotify_pipeline",
        start_date=datetime(2025, 1, 5),
        schedule_interval=timedelta(hours=2),
        catchup=False,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=45)
        }
    )

checar_data = BranchPythonOperator(
    task_id='choose_path_by_date',
    python_callable=decide_path_by_date,
    dag=dag
)

refresh_token = PythonOperator(
    task_id='refresh_spotify_token',
    python_callable=refresh_spotify_token,
    dag=dag
)

get_tracks_history = PythonOperator(
    task_id='get_spotify_history',
    python_callable=get_spotify_history,
    dag=dag
)

check_create_s3_folder = PythonOperator(
    task_id='create_s3_folder_if_not_exists',
    python_callable=create_s3_folder_if_not_exists,
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

check_folder = PythonOperator(
    task_id='check_s3_folder',
    python_callable=check_s3_folder,
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

open_files = PythonOperator(
    task_id='open_json_files',
    python_callable=open_json_files,
    dag=dag
)

extract_tracks = PythonOperator(
    task_id='extract_tracks',
    python_callable=extract_tracks_from_dynamodb,
    dag=dag
)

download_previews = PythonOperator(
    task_id='download_previews',
    python_callable=download_previews,
    dag=dag
)

extract_audio_features = PythonOperator(
    task_id='extract_audio_features',
    python_callable=extract_audio_features,
    dag=dag
)

insert_data = PythonOperator(
    task_id='insert_into_postgres',
    python_callable=insert_into_postgres,
    dag=dag
)

checar_data >> check_folder
checar_data >> check_create_s3_folder >> refresh_token >> get_tracks_history >> check_folder

check_folder >> open_files >> extract_tracks >> download_previews >> extract_audio_features >> insert_data


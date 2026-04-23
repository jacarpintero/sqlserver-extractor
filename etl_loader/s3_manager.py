"""
Gestor de AWS S3 para streaming de archivos
Optimizado para archivos grandes sin necesidad de descarga local
"""

import os
import io
import boto3
from typing import List, Dict, Optional, Tuple
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

load_dotenv()


class S3Manager:
    """Gestor de conexión y operaciones con AWS S3"""

    def __init__(self):
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.bucket_name = os.getenv('AWS_S3_BUCKET')

        self.s3_client = None
        self._initialize_client()

    def _initialize_client(self):
        try:
            if self.aws_access_key and self.aws_secret_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key,
                    aws_secret_access_key=self.aws_secret_key,
                    region_name=self.aws_region
                )
            else:
                # Usar IAM role o credenciales del sistema (EC2 instance profile)
                self.s3_client = boto3.client('s3', region_name=self.aws_region)

            self.s3_client.head_bucket(Bucket=self.bucket_name)

        except NoCredentialsError:
            raise Exception(
                "No se encontraron credenciales de AWS. "
                "Configura AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY en .env"
            )
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                raise Exception(f"El bucket '{self.bucket_name}' no existe")
            elif error_code == '403':
                raise Exception(f"No tienes permisos para acceder al bucket '{self.bucket_name}'")
            else:
                raise Exception(f"Error conectando a S3: {e}")

    def list_prefixes(self, prefix: str = "") -> List[str]:
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            prefixes = []
            if 'CommonPrefixes' in response:
                for p in response['CommonPrefixes']:
                    prefixes.append(p['Prefix'])
            return sorted(prefixes)
        except ClientError as e:
            raise Exception(f"Error listando prefijos en S3: {e}")

    def list_files(self, prefix: str = "", extensions: Tuple[str, ...] = ('.txt', '.csv')) -> List[Dict]:
        try:
            files = []
            paginator = self.s3_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                for obj in page['Contents']:
                    key = obj['Key']
                    if not key.endswith(extensions):
                        continue
                    files.append({
                        'key': key,
                        'name': key.split('/')[-1],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })

            return sorted(files, key=lambda x: x['name'])
        except ClientError as e:
            raise Exception(f"Error listando archivos en S3: {e}")

    def get_file_stream(self, s3_key: str):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            return response['Body']
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                raise Exception(f"Archivo no encontrado en S3: {s3_key}")
            else:
                raise Exception(f"Error obteniendo archivo de S3: {e}")

    def get_file_content(self, s3_key: str, encoding: str = 'latin1') -> str:
        stream = self.get_file_stream(s3_key)
        return stream.read().decode(encoding, errors='replace')

    def get_file_header_line(self, s3_key: str, encoding: str = 'latin1') -> str:
        """Lee solo la primera línea usando un range request (evita descargar el archivo completo)."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=s3_key, Range='bytes=0-8191'
            )
            chunk = response['Body'].read().decode(encoding, errors='replace')
            return chunk.split('\n', 1)[0]
        except Exception:
            return ''

    def count_file_lines(self, s3_key: str) -> int:
        """Cuenta líneas en streaming por chunks de 1MB sin cargar el archivo en RAM."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            count = 0
            for chunk in response['Body'].iter_chunks(chunk_size=1024 * 1024):
                count += chunk.count(b'\n')
            return max(0, count - 1)  # descontar header
        except Exception:
            return 0

    def file_exists(self, s3_key: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False

    def get_bucket_info(self) -> Dict:
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return {'name': self.bucket_name, 'region': self.aws_region, 'accessible': True}
        except ClientError:
            return {'name': self.bucket_name, 'region': self.aws_region, 'accessible': False}


class S3FileWrapper:
    """
    Wrapper para hacer que un stream de S3 se comporte como un archivo local.
    Compatible con psycopg2 COPY y context manager.
    """

    def __init__(self, s3_manager: S3Manager, s3_key: str, encoding: str = 'latin1'):
        self.s3_manager = s3_manager
        self.s3_key = s3_key
        self.encoding = encoding
        self._stream = None
        self._text_stream = None

    def __enter__(self):
        self._stream = self.s3_manager.get_file_stream(self.s3_key)
        self._text_stream = io.TextIOWrapper(
            self._stream,
            encoding=self.encoding,
            errors='replace'
        )
        return self._text_stream

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._text_stream:
            self._text_stream.close()
        if self._stream:
            self._stream.close()


def format_size(size_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

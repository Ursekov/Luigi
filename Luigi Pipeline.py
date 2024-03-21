import requests
import wget
import subprocess
import ssl
import pandas as pd
import luigi
from luigi.util import requires
import tarfile
import gzip
import shutil
import io
import os


class DownloadFiles(luigi.Task):
    dataset_name = luigi.Parameter(default='GSE68849_RAW.tar')

    def run(self):
        if 'Cache' not in os.listdir():
            subprocess.run(['mkdir', 'Cache']) # Создание основной папки, в которой буду храниться данные
        url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_name.rstrip('_RAW.tar')}&format=file"
        ssl._create_default_https_context = ssl._create_unverified_context
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            result = wget.download(url) # загрузка файла
        else:
            raise Exception(f"Не удалось скачать файл {self.dataset_name}")

        subprocess.run(['mv', result, 'Cache'])

    def output(self):
        return luigi.LocalTarget(f"Cache/{self.dataset_name}")


@requires(DownloadFiles)
class Unzipping_Transform(luigi.Task):
    cache = luigi.Parameter(default='Cache')
    files_names = []

    def run(self):
        tar = tarfile.open(f"{self.cache}/{self.dataset_name}", "r")
        self.files_names = tar.getnames()

        print(f'Количество файлов в архиве {self.dataset_name}: {len(tar.getmembers())}')
        print(f'Названия файлов в архиве {self.dataset_name}:\n{self.files_names[0]}\n{self.files_names[1]}')

        tar.extractall(self.cache) # извлечение файлов их архива

        for i in range(1, len(self.files_names) + 1):
            subprocess.run(['mkdir', f'Cache/{self.files_names[i-1].rstrip(".txtgz")}']) # создание папки для каждого файла
            with gzip.open(f'Cache/{self.files_names[i-1]}', 'rb') as f_in:
                with open(f'Cache/{self.files_names[i-1].rstrip(".txtgz")}/{self.files_names[i-1].rstrip(".gz")}', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out) # сохранение каждого файла в формате .txt
            subprocess.run(['rm', f'Cache/{self.files_names[i-1]}']) # удаление .gz архивов

        for name in self.files_names:
            dfs = {}
            with open(f'{self.cache}/{name.rstrip(".txtgz")}/{name.rstrip(".gz")}') as f:
                write_key = None
                fio = io.StringIO()
                for l in f.readlines():
                    if l.startswith('['):
                        if write_key:
                            fio.seek(0)
                            header = None if write_key == 'Heading' else 'infer'
                            dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                        fio = io.StringIO()
                        write_key = l.strip('[]\n')
                        continue
                    if write_key:
                        fio.write(l)
                fio.seek(0)
                dfs[write_key] = pd.read_csv(fio, sep='\t')
            for key, values in dfs.items(): # сохранение таблиц в отдельные файлы
                values.to_csv(f'{self.cache}/{name.rstrip(".txtgz")}/{key}.tsv', sep='\t')

    def output(self):
        return [
            luigi.LocalTarget(f'{self.cache}/GPL10558_HumanHT-12_V4_0_R1_15002873_B/Columns.tsv'),
            luigi.LocalTarget(f'{self.cache}/GPL10558_HumanHT-12_V4_0_R1_15002873_B/Controls.tsv'),
            luigi.LocalTarget(f'{self.cache}/GPL10558_HumanHT-12_V4_0_R1_15002873_B/Heading.tsv'),
            luigi.LocalTarget(f'{self.cache}/GPL10558_HumanHT-12_V4_0_R1_15002873_B/Probes.tsv'),
            luigi.LocalTarget(f'{self.cache}/GPL10558_HumanHT-12_V4_0_R2_15002873_B/Columns.tsv'),
            luigi.LocalTarget(f'{self.cache}/GPL10558_HumanHT-12_V4_0_R2_15002873_B/Controls.tsv'),
            luigi.LocalTarget(f'{self.cache}/GPL10558_HumanHT-12_V4_0_R2_15002873_B/Heading.tsv'),
            luigi.LocalTarget(f'{self.cache}/GPL10558_HumanHT-12_V4_0_R2_15002873_B/Probes.tsv')
        ]


@requires(Unzipping_Transform)
class PandasTime(luigi.Task):
    file_input = luigi.Parameter(default='Probes.tsv')
    file_output = luigi.Parameter(default='Stripped_Probes.tsv')
    drop_features = luigi.Parameter(default=['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence'])

    def run(self):
        tar = tarfile.open(f"{self.cache}/{self.dataset_name}", "r")
        self.files_names = tar.getnames()
        for name in self.files_names: # создание урезанного файла
            probes = pd.read_csv(f'{self.cache}/{name.rstrip(".txtgz")}/{self.file_input}', sep='\t')
            stripped_probes = probes.drop([*self.drop_features], axis=1)
            stripped_probes.to_csv(f'{self.cache}/{name.rstrip(".txtgz")}/{self.file_output}', sep='\t')


    def output(self):
        return [
            luigi.LocalTarget(f'{self.cache}/GPL10558_HumanHT-12_V4_0_R1_15002873_B/Stripped_Probes.tsv'),
            luigi.LocalTarget(f'{self.cache}/GPL10558_HumanHT-12_V4_0_R2_15002873_B/Stripped_Probes.tsv')
        ]


@requires(PandasTime)
class DelFiles(luigi.Task):
    def run(self):
        ex_file = subprocess.run(['ls', f'{self.cache}'], capture_output=True)
        if self.dataset_name in ex_file.stdout.decode('utf-8').splitlines(): # проверка существования файла, с которым происходит работа
            tar = tarfile.open(f"{self.cache}/{self.dataset_name}", "r")
            self.files_names = tar.getnames()
            subprocess.run(['rm', f'{self.cache}/{self.dataset_name}'])
            for name in self.files_names:
                subprocess.run(['rm', f'{self.cache}/{name.rstrip(".txtgz")}/{name.rstrip(".gz")}'])


if __name__ == '__main__':
    luigi.run()

